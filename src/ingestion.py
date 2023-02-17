"""Uploads database table data to separate 'csv' files on S3."""
import logging
import pg8000.native as pg
import pg8000.exceptions as pge
import boto3
import botocore.exceptions as be
import pandas as pd
from io import StringIO
import json


logger = logging.getLogger("ingestion")
logger.setLevel(logging.INFO)

s3_resource = boto3.resource('s3')
secrets = boto3.client('secretsmanager')


def lambda_handler(event, context):
    """ Handles functions to pull data from database and upload as a csv file to S3
    Args:
        event:
    
        context:

    Raises:
    """
    tables_list = ['staff', 'transaction', 'design', 'address', 
                    'sales_order', 'counterparty', 'payment', 
                    'payment_type', 'currency', 'department', 
                    'purchase_order']
    bucket_name = 'ingest-bucket-totedd-140220230217153255264700000002' # INSERT BUCKET NAME HERE
    
    try:
        columns = collect_column_headers(tables_list)
        bucket_key = get_keys_from_table_names(tables_list)
        for index, table in enumerate(tables_list):
            data_to_bucket_csv_file(table, columns[index], 
                                    bucket_name, bucket_key[index])
        logger.info('SUCCESSFUL INGESTION')
        print('SUCCESSFUL INGESTION')
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


def get_secret_value(secret_name: str) -> dict:
    """ Finds data for a specified secret on SecretsManager
    Args:
        secret_id: The Secret Name that holds the username and password
                    for your data base
    Returns:
        Dictionary containing data on secret
    Raises:
        DatabaseError
    """
    try:
        secret_value = secrets.get_secret_value(SecretId = secret_name)
        secrets_dict = json.loads(secret_value["SecretString"])
        return(secrets_dict)
    except secrets.exceptions.ResourceNotFoundException as e:
        logger.critical(f'The requested secret {secret_name} was not found')
        raise e
    except be.ParamValidationError as e:
        logger.critical('The request has invalid params')
        raise e
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


def get_connection():
    HOST = get_secret_value('database_credentials')['host'] # INSERT SECRET NAME HERE
    PORT = get_secret_value('database_credentials')['port'] # INSERT SECRET NAME HERE
    USER = get_secret_value('database_credentials')['user'] # INSERT SECRET NAME HERE
    PASS = get_secret_value('database_credentials')['password'] # INSERT SECRET NAME HERE
    DATABASE = get_secret_value('database_credentials')['database'] # INSERT SECRET NAME HERE
    try:
        return pg.Connection(USER, password = PASS, database = DATABASE, host = HOST, port = PORT)
    except pge.DatabaseError:
        logger.critical('DatabaseError: Unable to connect to database')
        raise pge.DatabaseError('DatabaseError: Unable to connect to database')
    except Exception as e:
        logger.critial(e)
        raise RuntimeError


def get_keys_from_table_names(tables, file_path = ''):
    """ Appends '.csv' to items in list.
    Args:
        tables: A list of table names.
        file_path : Add a file path for a folder-like structure in S3. (OPTIONAL)
    Returns:
        A list of table names with appended file extension.
    Raises:
    """
    return [f'{file_path}{table_name}.csv' for table_name in tables]
    


def sql_select_column_headers(table, conn):
    """ Queries database find column headers for a table.
    Args:
        table: The name of a table.
    Returns:
        A collection of nested lists containing table headers.
    Raises:
    """
    conn.run(f'SELECT * FROM {table};')
    return ([column['name'] for column in conn.columns])


def collect_column_headers(tables):
    """ Collects column headers from sql query into a list.
    Args:
        table: A list of table names.
    Returns:
        A collection of nested lists containing table headers.
    """
    table_headers_list = []
    for table in tables:
        table_headers_list.append(sql_select_column_headers(table, get_connection()))
    return table_headers_list


def sql_select_query(table, conn):
    """ Queries database to select all data from a table.
    Args:
        table: The name of the table to get data from. 
    Returns:
        A collection of nested lists of row data
    Raises:
    """
    return conn.run(f'SELECT * FROM {table};')


def data_to_bucket_csv_file(table_name, column_headers, bucket_name, bucket_key):
    """ Takes data collected from 'sql_get_all_data' function 
        and uploads it to S3 as a csv file.
    Args:
        table_name: The name of the table to get data from.
        column_headers: A collection of nested lists containing table headers.
        bucket_name: The name of the bucket in S3.
        bucket_key: The name of the file and path the data will be stored in.
    Returns:
        Formated data as a list of dictionaries.
    Raises:
    """
    try:
        data_from_table = sql_select_query(table_name, get_connection())
        rows_list = []
        for row in data_from_table:
            row_data_dict = {}
            for index, column in enumerate(column_headers):
                row_data_dict[column] = row[index]
            rows_list.append(row_data_dict)
        df = pd.DataFrame(data = rows_list, columns = column_headers)
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        s3_resource.Object(bucket_name, bucket_key).put(Body = csv_buffer.getvalue())
        return rows_list
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


if __name__ == "__main__":
    lambda_handler({}, {})