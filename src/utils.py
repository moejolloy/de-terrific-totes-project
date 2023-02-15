import boto3
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger("processing")
logger.setLevel(logging.INFO)

"""
Function accepts a CSV file
Reformats file and outputs as parquet
"""
s3 = boto3.client('s3')


def load_from_s3(bucket, key):
    try:
        s3_response_object = s3.get_object(
            Bucket=bucket, Key=key)
        df = s3_response_object['Body'].read()
        df = pd.read_csv(BytesIO(df))
        return df
    except s3.exceptions.NoSuchBucket:
        logger.critical('Bucket does not exist')
    except s3.exceptions.NoSuchKey:
        logger.critical("Key not found in bucket")
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


print(load_from_s3('nc-marie-c-demo', 'Results.csv'))
