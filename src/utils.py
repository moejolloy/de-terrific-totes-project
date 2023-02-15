import boto3
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger("processing")
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def load_csv_from_s3(bucket, key):
    """ Retrieve a CSV file from an S3 bucket
    Args:
        bucket: Name of the S3 bucket from which to retrieve the file.
        key: Key that the file is stored under in the named S3 bucket.

    Returns:
        DataFrame containing the contents of the CSV file.
    """
    try:
        s3_response_object = s3.get_object(
            Bucket=bucket, Key=key)
        df = s3_response_object['Body'].read()
        df = pd.read_csv(BytesIO(df))
        return df
    except s3.exceptions.NoSuchBucket:
        logger.critical('Bucket does not exist')
        raise s3.exceptions.NoSuchBucket({}, '')
    except s3.exceptions.NoSuchKey:
        logger.critical("Key not found in bucket")
        raise s3.exceptions.NoSuchKey({}, '')
    except Exception as e:
        logger.critical(e)
        raise RuntimeError
