import os
import boto3
import logging
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME           = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION    = os.getenv('AWS_DEFAULT_REGION')
LOCAL_DATA_DIR = "data"

# Configuramos el logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/macro_logs.log', encoding='utf-8'),
        logging.StreamHandler()  # Para imprimir en pantalla los logs
    ]
)

logger = logging.getLogger(__name__)

if __name__=="__main__":
    try:
        # Consiguramos cliente de S3
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
        logger.info(f"Conexión exitosa con el bucket {BUCKET_NAME}")

        local_files = []
        data_path =Path(LOCAL_DATA_DIR)
        
        for ext in ('*.csv', '*.json'):
            local_files.extend(data_path.rglob(ext))

        logger.info(f"Subiendo {len(local_files)} archivos...")
        for local_file in local_files:
            content_type = 'text/csv' if local_file.suffix == '.csv' else 'application/json'
            
            s3.upload_file(
                str(local_file),
                BUCKET_NAME,
                str(local_file),
                ExtraArgs={'ContentType':content_type}
            )
            logger.info(f"Subido: {local_file}")
        logger.info(f"Archivos subidos con éxito")

    except Exception as e:
        logger.error("Error al intentar subir los archivos al {BUCKET_NAME}: {e}")
        exit(1)

    





