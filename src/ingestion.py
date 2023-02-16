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


def get_secret_value(secret_id: str) -> dict:
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
        secret_value = secrets.get_secret_value(SecretId = secret_id)
        secrets_dict = json.loads(secret_value["SecretString"])
        return(secrets_dict)
    except secrets.exceptions.ResourceNotFoundException as e:
        logger.critical(f'The requested secret {secret_id} was not found')
        raise e
    except be.ParamValidationError as e:
        logger.critical('The request has invalid params')
        raise e
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


def get_connection(user, password, database, host, port):
    """ Creates a connection to a PostgreSQL database.
    Args:
        user: The username to connect to the PostgreSQL server with.
        password: The user password to connect to the server with.
        database: The name of the database instance to connect with.
        host: The hostname of the PostgreSQL server to connect with.
        port: The TCP/IP port of the PostgreSQL server instance.
    Returns:
        None
    Raises:
        DatabaseError
    """
    try:
        conn = pg.Connection(
                            user, 
                            host = host, 
                            port = port , 
                            password = password, 
                            database = database
                        )
        return conn
    except pge.DatabaseError:
        logger.critical('DatabaseError: Unable to connect to database')
        raise pge.DatabaseError('DatabaseError: Unable to connect to database')
    except Exception as e:
        logger.critial(e)
        raise RuntimeError


def get_keys_from_table_names(tables):
    """ Appends '.csv' to items in list.
    Args:
        tables: A list of table names.
    Returns:
        A list of table names with appended file extension.
    Raises:
    """
    keys_list = []
    for table_name in tables:
        keys_list.append(f'{table_name}.csv')
    return keys_list


def sql_get_column_headers(conn, tables):
    """ Queries database find column headers for a list of tables.
    Args:
        tables: A list of table names.
    Returns:
        A collection of nested lists containing table headers.
    Raises:
    """
    table_headers_list = []
    for table in tables:
        conn.run(f'SELECT * FROM {table};')
        table_headers_list.append([column['name'] for column in conn.columns])
    return table_headers_list


def sql_get_all_data(conn, table_name):
    """ Queries database to select all data from a table.
    Args:
        table_name: The name of the table to get data from. 
    Returns:
        A collection of nested lists of row data
    Raises:
    """
    table_data = conn.run(f'SELECT * FROM {table_name};')
    return table_data


def data_to_bucket_csv_file(conn, table_name, column_headers, bucket_name, bucket_key):
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
        data_from_table = sql_get_all_data(conn, table_name)
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


def lambda_handler(event, context):
    """ Runs functions required to upload all table data to S3
    Returns:
        None
    Raises:
    """
    HOST = (f'nc-data-eng-totesys-production.chpsczt8h1nu.'
        f'eu-west-2.rds.amazonaws.com')
    PORT = 5432
    USER = get_secret_value('')['username'] # INSERT SECRET NAME HERE
    PASS = get_secret_value('')['password'] # INSERT SECRET NAME HERE
    DATABASE = 'totesys'
    tables_list = ['staff', 'transaction', 'design', 'address', 
                    'sales_order', 'counterparty', 'payment', 
                    'payment_type', 'currency', 'department', 
                    'purchase_order']
    bucket_name = '' # INSERT BUCKET NAME HERE
    
    conn = get_connection(USER, PASS, DATABASE, HOST, PORT)
    try:
        columns = sql_get_column_headers(conn, tables_list)
        bucket_key = get_keys_from_table_names(tables_list)
        for index, table in enumerate(tables_list):
            data_to_bucket_csv_file(conn, table, columns[index], 
                                    bucket_name, bucket_key[index])
        logger.info('SUCCESSFUL INGESTION')
        print('SUCCESSFUL INGESTION')
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


# if __name__ == "__main__":
#     lambda_handler({}, {})