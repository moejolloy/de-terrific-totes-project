""" Uploads database table data to separate '.csv' files on AWS S3.

The script consists of several functions that connect to either
a hosted sql database or an AWS resource to assist with uploading data
to S3.

The script is intended to run on AWS Lambda.
"""

import logging
from pg8000.native import Connection
import pg8000.exceptions as pge
import boto3
import botocore.exceptions
import botocore.errorfactory
import pandas as pd
from io import StringIO
import json
import os


logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)

s3_resource = boto3.resource("s3")
s3_client = boto3.client("s3")
secrets = boto3.client("secretsmanager")


def lambda_handler(event, context):
    """ Handles functions to pull data from a database to upload as a
    csv file to S3.
    Checks if the data exists on s3 and if the table has been updated
    since the last interval before uploading.

    Args:
        event: An AWS event object.
        context: A valid AWS lambda Python context object.
    """
    credentials = get_secret_value('database_credentials')
    TABLES_LIST = ['staff', 'transaction', 'design', 'address',
                   'sales_order', 'counterparty', 'payment',
                   'payment_type', 'currency', 'department',
                   'purchase_order']
    BUCKET = os.environ.get('TF_ING_BUCKET')
    INTERVAL = '3 minutes'
    has_updated = False
    columns = collect_column_headers(credentials, TABLES_LIST)
    bucket_keys = get_keys_from_table_names(TABLES_LIST)
    is_data_on_s3 = check_key_exists(BUCKET, bucket_keys[0])
    for index, table in enumerate(TABLES_LIST):
        if sql_check_updated(credentials, table,
                             INTERVAL) or not is_data_on_s3:

            data_to_bucket_csv_file(
                credentials, table, columns[index], BUCKET,
                bucket_keys[index]
            )
            has_updated = True

    if has_updated:
        logger.info("SUCCESSFUL INGESTION")
    else:
        logger.info("NO FILES TO UPDATE")


def get_secret_value(secret_name):
    """ Finds data for a specified secret on SecretsManager.

    Args:
        secret_id: The Secret Name that holds the username and password
        for your database.

    Returns:
        Dictionary containing data on secret.

    Raises:
        ResourseNotFoundException
        ParamValidationError
        UnrecognizedClientException
        RuntimeError
    """
    try:
        secret_value = secrets.get_secret_value(SecretId=secret_name)
    except secrets.exceptions.ResourceNotFoundException as e:
        logger.error(f"The requested secret {secret_name} was not found")
        raise e
    except botocore.exceptions.ParamValidationError as e:
        logger.error("The request has invalid params")
        raise e
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "UnrecognizedClientException":
            logger.error("Security token invalid, check permissions")
            raise e
    except Exception as e:
        logger.error(e)
        raise RuntimeError
    else:
        secrets_dict = json.loads(secret_value["SecretString"])
        return secrets_dict


def get_connection(credentials):
    """ Attempts connection with database.

    Args:
        credentials: The credentials required to access the database
        stored in secretsmanager as a dictionary.

    Returns:
        An instance of the Connection Class.

    Raises:
        InterfaceError
        RuntimeError
    """
    try:
        HOST = credentials["host"]
        PORT = credentials["port"]
        USER = credentials["user"]
        PASS = credentials["password"]
        DATABASE = credentials["database"]
    except KeyError as e:
        logger.error("Credentials key not in secret")
        raise e
    else:
        try:
            return Connection(USER, password=PASS, database=DATABASE,
                              host=HOST, port=PORT)
        except pge.InterfaceError as e:
            logger.error(e)
            raise e


def get_keys_from_table_names(tables, file_path=""):
    """ Appends '.csv' to items in list.

    Args:
        tables: A list of table names.
        (OPTIONAL) file_path : Add a file path for a folder-like
        structure in S3.

    Returns:
        A list of table names with appended file extension.
    """
    return [f"{file_path}{table_name}.csv" for table_name in tables]


def sql_select_column_headers(credentials, table):
    """ Queries database find column headers for a table.

    Args:
        credentials: The credentials required to access the database
        stored in secretsmanager as a dictionary.
        table: The name of a table.

    Returns:
        A list of column headers for that table.

    Raises:
        Database Error
    """
    conn = get_connection(credentials)
    try:
        conn.run(f"SELECT * FROM {table} LIMIT 0;")
    except pge.DatabaseError as e:
        logger.error(f"DatabaseError: {table} does not exist in database")
        raise e
    else:
        return [column['name'] for column in conn.columns]


def collect_column_headers(credentials, tables):
    """ Collects column headers from sql query into a list.

    Args:
        credentials: The credentials required to access the database.
        stored in secretsmanager as a dictionary.
        table: A list of table names.

    Returns:
        A collection of nested lists containing all table headers.
    """
    table_headers_list = []
    for table in tables:
        column_header = sql_select_column_headers(credentials, table)
        logging.info(column_header)
        table_headers_list.append(column_header)

    return table_headers_list


def sql_select_query(credentials, table):
    """ Queries database to select all data from a table.

    Args:
        credentials: The credentials required to access the database
        stored in secretsmanager as a dictionary.
        table: The name of the table to get data from.

    Returns:
        A collection of nested lists of row data

    Raises:
        DatabaseError
    """
    conn = get_connection(credentials)
    try:
        return conn.run(f"SELECT * FROM {table};")
    except pge.DatabaseError as e:
        logger.error(f"DatabaseError: {table} does not exist in database")
        raise e


def sql_check_updated(credentials, table, interval):
    """ Queries database to check a table has been updated since the
    last interval.

    Args:
        credentials: The credentials required to access the database
        stored in secretsmanager as a dictionary.
        table: The name of the table to get data from.
        interval: A length of time going backwards from 'now' to check
        against the 'last_updated' column.

    Returns:
        A boolean for whether the table has been updated.

    Raises:
        DatabaseError
    """
    conn = get_connection(credentials)
    try:
        updated = conn.run(
            f"SELECT last_updated FROM {table} WHERE "
            f"last_updated > now() - INTERVAL '{interval}' LIMIT 1;"
        )
    except pge.DatabaseError as e:
        logger.error(f"DatabaseError: {table} does not exist in database")
        raise e
    else:
        return True if len(updated) != 0 else False


def check_key_exists(bucket_name, bucket_key):
    """ Checks if key exists in s3.

    Args:
        bucket_name: The name of the bucket containing the file.
        bucket_key: The filepath and name of the file in s3.

    Returns:
        A boolean for whether the key exists.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=bucket_key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
    else:
        return True


def data_to_bucket_csv_file(
    credentials, table_name, column_headers, bucket_name, bucket_key
):
    """ Takes data collected from 'sql_get_all_data' function
        and uploads it to S3 as a csv file.

    Args:
        table_name: The name of the table to get data from.
        column_headers: A collection of nested lists containing
        table headers.
        bucket_name: The name of the bucket in S3.
        bucket_key: The name of the file and path the data will
        be stored in.

    Returns:
        Formated data as a list of dictionaries.

    Raises:
        NoSuchBucket
        ParamValidationError
    """
    data_from_table = sql_select_query(credentials, table_name)
    rows_list = []
    for row in data_from_table:
        row_data_dict = {}
        for index, column in enumerate(column_headers):
            row_data_dict[column] = row[index]
        rows_list.append(row_data_dict)
    df = pd.DataFrame(data=rows_list, columns=column_headers)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_resource.Object(bucket_name, bucket_key).put(
            Body=csv_buffer.getvalue())
    except botocore.errorfactory.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            logger.error(f"{bucket_name} does not exist in your S3")
            raise e
    except botocore.exceptions.ParamValidationError as e:
        logger.error("The request has invalid params")
        raise e
    else:
        return rows_list
