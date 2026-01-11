import gzip
import os
from io import BytesIO

import boto3
from boto3.s3.transfer import ClientError
from botocore.exceptions import ClientError

from apientreprises import logger
from apientreprises.models import SettingsModel

GZIP_CONTENT_TYPES = (
    "text/css",
    "application/javascript",
    "application/x-javascript",
    "text/javascript",
)


async def create_s3_connection(settings: SettingsModel = None, region_name: str | None = None, s3_host: str | None = None):
    session_config = {
        'aws_access_key_id': os.getenv('AWS_S3_ACCESS_KEY_ID'),
        'aws_secret_access_key': os.getenv('AWS_S3_SECRET_ACCESS_KEY'),
    }

    if region_name:
        session_config['region_name'] = region_name

        session = boto3.Session(**session_config)

        client_config = {}

        if s3_host:
            if not s3_host.startswith(('http://', 'https://')):
                s3_host = f'https://{s3_host}'

            client_config['endpoint_url'] = s3_host
            # Configure for S3-compatible services
            # that use path-style addressing
            client_config['config'] = boto3.session.Config(
                s3={
                    'addressing_style': 'path'
                }
            )

        client = session.client('s3', **client_config)
        resource = session.resource('s3', **client_config)

        # This is a trap because if always returns an object
        # even if the bucket does not exist. We need to explicitly
        # test if the bucket exists by calling head_bucket -- see below
        bucket = resource.Bucket(os.getenv('AWS_STORAGE_BUCKET_NAME'))

        try:
            client.head_bucket(Bucket=os.getenv('AWS_STORAGE_BUCKET_NAME'))
            logger.info(
                f"✅ Connected to existing bucket: {os.getenv('AWS_STORAGE_BUCKET_NAME')}")
        except ClientError as e:
            # If the bucket does not exist we receive
            # a 404 code and that will be the trigger
            # that will be using to create a new bucket
            error_code = e.response['Error']['Code']

            if error_code == '404':
                logger.warning(
                    f"🔴 Bucket {os.getenv('AWS_STORAGE_BUCKET_NAME')} "
                    "not found. Attempting to create..."
                )

                try:
                    create_bucket_config = {}

                    # For regions other than us-east-1, we need to specify LocationConstraint
                    if os.getenv('AWS_S3_REGION_NAME') and os.getenv('AWS_S3_REGION_NAME') != 'us-east-1':
                        create_bucket_config['CreateBucketConfiguration'] = {
                            'LocationConstraint': os.getenv('AWS_S3_REGION_NAME')
                        }

                    client.create_bucket(
                        Bucket=os.getenv('AWS_STORAGE_BUCKET_NAME'),
                        **create_bucket_config
                    )

                    waiter = client.get_waiter('bucket_exists')
                    waiter.wait(
                        Bucket=os.getenv('AWS_STORAGE_BUCKET_NAME'),
                        WaiterConfig={'Delay': 2, 'MaxAttempts': 30}
                    )

                    logger.info(
                        f"🟢 Created bucket: {os.getenv('AWS_STORAGE_BUCKET_NAME')}")
                except ClientError as creation_error:
                    raise Exception(
                        f"🔴 Failed to create bucket '{os.getenv('AWS_STORAGE_BUCKET_NAME')}': "
                        f"{creation_error.response['Error']['Message']}"
                    )
            elif error_code == '403':
                raise Exception(
                    f"🔴 Access denied to bucket '{os.getenv('AWS_STORAGE_BUCKET_NAME')}'. "
                    f"Check your AWS credentials and bucket permissions."
                )
            else:
                raise Exception(
                    f"🔴 Failed to access bucket '{os.getenv('AWS_STORAGE_BUCKET_NAME')}': "
                    f"{e.response['Error']['Message']}"
                )

        logger.info(f"✅ Uploads will go to bucket: {bucket.name}")

        return client, bucket


def compress_string(content: str):
    """Helper function that Gzips a given
    string by doing xyz"""
    buffer = BytesIO()
    gzip_file = gzip.GzipFile(mode='wb', compresslevel=6, fileobj=buffer)
    gzip_file.write(content)
    buffer.seek(0)
    gzip_file.close()
    return buffer.getvalue()
