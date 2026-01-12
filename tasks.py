import json
import mimetypes
import os
import asgiref
import asgiref.sync
import pandas
from typing import Any
import uuid
from asgiref.sync import async_to_sync
from boto3.s3 import transfer
from boto3.s3.transfer import ClientError
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from celery import shared_task
from celery.utils.log import get_task_logger

from apientreprises import BASE_DIR, DATA_DIR
from apientreprises.uploading import (GZIP_CONTENT_TYPES, compress_string,
                                      create_s3_connection)
from utils import write_to_json

celery_logger = get_task_logger(__name__)


@shared_task
def upload_to_storage(filename: str, renamegzip: bool = False):
    client, bucket = async_to_sync(create_s3_connection)()

    extra_args = {'ContentType': 'application/octet-stream'}
    extra_args.update(
        **{
            'ACL': 'public-read'
        }
    )

    content_type = mimetypes.guess_type(filename)[0]
    if content_type:
        extra_args.update(**{'ContentType': content_type})

    fullpath = BASE_DIR / filename

    with open(fullpath, mode='rb') as f:
        file_size = os.fstat(f.fileno()).st_size
        filedata = f.read()

        # Gzip only if file is large enough (>1K is recommended)
        # and only if file is a common text type (not a binary file)
        file_constraints = all([
            file_size > 1024,
            content_type in GZIP_CONTENT_TYPES
        ])

        if file_constraints:
            file_data = compress_string(file_data)
            if renamegzip:
                # If rename_gzip is True, then rename the file
                # by appending an extension (like '.gz)' to
                # original filename
                file_key = f'{file_key}.{os.getenv("SYNC_S3_RENAME_GZIP_EXT")}'

            extra_args["ContentEncoding"] = 'gzip'

            celery_logger.warning(
                f"Gzipped file: {file_size / 1024} "
                f"to {file_data / 1024}"
            )

        try:
            if file_size > 100 * 1024 * 1024:  # 100MB threshold
                config = transfer.TransferConfig(
                    multipart_threshold=1024 * 25,  # 25MB
                    max_concurrency=10,
                    multipart_chunksize=1024 * 25,
                    use_threads=True
                )

                client.upload_file(
                    Filename=str(fullpath),
                    Bucket=os.getenv('AWS_STORAGE_BUCKET_NAME'),
                    Key=filename,
                    ExtraArgs=extra_args,
                    Config=config
                )
            else:
                client.upload_file(
                    Filename=str(fullpath),
                    Bucket=os.getenv('AWS_STORAGE_BUCKET_NAME'),
                    Key=filename,
                    ExtraArgs=extra_args
                )
            celery_logger.info(f"🟢 Uploaded file: {filename} to S3 bucket")
        except ClientError as e:
            celery_logger.error(f"🔴 Failed to upload file: {filename}")
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            celery_logger.error(f"🔴 AWS S3 upload error: {e}")
            raise
        else:
            celery_logger.info(f"✅ Uploaded file: {filename} to S3 bucket")


@shared_task
def clean_data(data: dict[str, Any]):
    df = pandas.DataFrame(data)
    return json.loads(df.to_json(orient='records'))


@shared_task
def create_file(data: dict[str, Any]):
    filename = DATA_DIR / f"{uuid.uuid4()}.json"
    asgiref.sync.async_to_sync(write_to_json)(data, filename)
    celery_logger.info(f"✅ Created JSON file: {filename}")

# conn = async_to_sync(redis_connection)()
# data = async_to_sync(conn.xread)('responses', count=1000, block=5000)
# return {}
