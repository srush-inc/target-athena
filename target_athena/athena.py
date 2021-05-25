import os
import singer
from logging import Logger
from pyathena import connect


def create_client(config, logger: Logger):
    logger.info("Attempting to create Athena session")

    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get("aws_access_key_id") or os.environ.get(
        "AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key = config.get("aws_secret_access_key") or os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    )
    aws_session_token = config.get("aws_session_token") or os.environ.get(
        "AWS_SESSION_TOKEN"
    )
    aws_profile = config.get("aws_profile") or os.environ.get("AWS_PROFILE")
    aws_region = config.get("aws_region") or os.environ.get("AWS_REGION")
    s3_staging_dir = config.get("s3_staging_dir") or os.environ.get("S3_STAGING_DIR")
    logger.info(f"Using Athena region {aws_region}")

    # AWS credentials based authentication
    if aws_access_key_id and aws_secret_access_key:
        cursor = connect(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region,
            s3_staging_dir=s3_staging_dir,
        ).cursor()

    # AWS Profile based authentication
    else:
        cursor = connect(
            profile_name=aws_profile,
            region_name=aws_region,
            s3_staging_dir=s3_staging_dir,
        ).cursor()
    return cursor


def execute_sql(sql, athena_client):
    athena_client.execute(sql)
    # print(cursor.description)
    # print(cursor.fetchall())