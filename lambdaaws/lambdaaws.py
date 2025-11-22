import boto3
import json
import traceback
from urllib.parse import unquote_plus

s3 = boto3.client("s3")

def count_lowercase_letters(text):
    # Conta letras minúsculas unicode (inclui á, ç, õ etc).
    # Garantimos que só conte letras (isalpha) que também são islower.
    return sum(1 for ch in text if ch.isalpha() and ch.islower())

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

        # Visualização de amostra (primeiros 200 chars) para debug
        sample = raw_bytes[:1000].decode("utf-8", errors="replace")
        print("Trecho inicial do arquivo (apenas para debug):")
        print(sample[:200].replace("\n", "\\n"))

        # Decodifica para texto
        text = raw_bytes.decode("utf-8", errors="replace")

        # Conta minúsculas
        total_minusculas = count_lowercase_letters(text)

        mensagem = f"O arquivo '{file_key}' contém {total_minusculas} letras minúsculas."
        print(mensagem)

        return {
            "bucket": bucket_name,
            "key": file_key,
            "bytes": len(raw_bytes),
            "minusculas": total_minusculas,
            "mensagem": mensagem,
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
