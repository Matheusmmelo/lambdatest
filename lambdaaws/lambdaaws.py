import boto3
import json
import traceback
import os
from urllib.parse import unquote_plus
from datetime import datetime

# --- Configurações Globais e Clientes AWS ---

# Leitura das variáveis de ambiente
TABLE_NAME = os.environ.get("AWS_DYNAMODB_TABLE_TARGET_NAME_0")
TABLE_REGION = os.environ.get("AWS_DYNAMODB_TABLE_TARGET_REGION_0")

# Inicializa clientes
s3 = boto3.client("s3")

# Inicializa DynamoDB (Resource é mais fácil para operações de CRUD como put_item)
# Se a região não for definida, ele usará a região padrão da Lambda
dynamodb = boto3.resource("dynamodb", region_name=TABLE_REGION)

# Referência para a tabela
# Nota: Isso é instanciado fora do handler para aproveitar o reaproveitamento de conexão da Lambda
try:
    if TABLE_NAME:
        table = dynamodb.Table(TABLE_NAME)
    else:
        table = None
        print("AVISO: Variável AWS_DYNAMODB_TABLE_TARGET_NAME_0 não definida.")
except Exception as e:
    print(f"Erro ao inicializar recurso DynamoDB: {e}")
    table = None

def count_lowercase_letters(text):
    # Conta letras minúsculas unicode (inclui á, ç, õ etc).
    return sum(1 for ch in text if ch.isalpha() and ch.islower())

def save_to_dynamodb(file_key, bucket_name, count):
    """
    Salva o resultado na tabela DynamoDB.
    Chave de Partição: ID (baseado no nome do arquivo)
    """
    if not table:
        print("Tabela DynamoDB não configurada. Pulando salvamento.")
        return

    try:
        print(f"Salvando dados no DynamoDB: Tabela={TABLE_NAME}, ID={file_key}")
        
        item = {
            "ID": file_key,                 # Chave de partição solicitada
            "Bucket": bucket_name,
            "MinusculasCount": count,       # O resultado da soma
            "ProcessedAt": str(datetime.now())
        }
        
        table.put_item(Item=item)
        print("Item salvo com sucesso no DynamoDB.")
        
    except Exception as e:
        print(f"Erro ao gravar no DynamoDB: {str(e)}")
        # Opcional: relançar a exceção se quiser que a Lambda falhe caso o banco falhe
        # raise e 

def process_record(record):
    try:
        s3_info = record.get("s3")
        if not s3_info:
            raise ValueError("Record sem campo 's3'")

        bucket_name = s3_info["bucket"]["name"]
        file_key_raw = s3_info["object"]["key"]
        file_key = unquote_plus(file_key_raw)

        print(f"--- Processando arquivo ---")
        print(f"Bucket: {bucket_name}")
        print(f"Raw key: {file_key_raw}")
        print(f"Decoded key: {file_key}")

        # Baixa o objeto
        resp = s3.get_object(Bucket=bucket_name, Key=file_key)
        raw_bytes = resp["Body"].read()
        print(f"Tamanho (bytes): {len(raw_bytes)}")

        # Decodifica para texto
        text = raw_bytes.decode("utf-8", errors="replace")

        # Conta minúsculas
        total_minusculas = count_lowercase_letters(text)

        mensagem = f"O arquivo '{file_key}' contém {total_minusculas} letras minúsculas."
        print(mensagem)

        # --- NOVA FUNCIONALIDADE: Salvar no DynamoDB ---
        save_to_dynamodb(file_key, bucket_name, total_minusculas)
        # -----------------------------------------------

        return {
            "bucket": bucket_name,
            "key": file_key,
            "bytes": len(raw_bytes),
            "minusculas": total_minusculas,
            "mensagem": mensagem,
            "db_status": "saved" if table else "skipped_config",
            "status": "ok"
        }

    except Exception as e:
        tb = traceback.format_exc()
        print("Erro ao processar record:", str(e))
        print(tb)
        return {
            "bucket": s3_info["bucket"]["name"] if s3_info and "bucket" in s3_info else None,
            "key": s3_info["object"]["key"] if s3_info and "object" in s3_info else None,
            "status": "error",
            "erro": str(e),
            "traceback": tb
        }

def lambda_handler(event, context):
    print("Evento recebido (raw):")
    try:
        print(json.dumps(event, ensure_ascii=False))
    except Exception:
        print(str(event))

    results = []

    # Validar Records
    records = event.get("Records")
    if not records:
        msg = "Evento sem 'Records' - verifique o tipo de trigger (deve ser S3 ObjectCreated)."
        print(msg)
        return {
            "statusCode": 400,
            "body": json.dumps({"erro": msg}, ensure_ascii=False)
        }

    # Processa todos os records
    for idx, record in enumerate(records):
        print(f"\nProcessando record #{idx}")
        result = process_record(record)
        results.append(result)

    # Retorna resumo
    return {
        "statusCode": 200,
        "body": json.dumps({"results": results}, ensure_ascii=False)
    }