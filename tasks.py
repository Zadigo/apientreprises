import mimetypes
import os

from asgiref.sync import async_to_sync
from boto3.s3 import transfer
from boto3.s3.transfer import ClientError
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from celery import shared_task

from apientreprises import BASE_DIR, logger
from apientreprises.uploading import (GZIP_CONTENT_TYPES, compress_string,
                                      create_s3_connection)


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

            logger.info(
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
        except ClientError as e:
            logger.error(f"🔴 Failed to upload file: {filename}")
        except (ClientError, NoCredentialsError, BotoCoreError) as e:
            logger.error(f"🔴 AWS S3 upload error: {e}")
            raise
        else:
            logger.info(f"✅ Uploaded file: {filename} to S3 bucket")
