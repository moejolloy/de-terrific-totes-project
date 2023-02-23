import logging
import boto3
import botocore
import json
import pandas as pd
from io import BytesIO
import psycopg2
import psycopg2.extras


logger = logging.getLogger("population")
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """ Retrieves and reads parquet files from S3, and inserts the
        data into the Data Warehouse.

    Args:
        event: an AWS event object.
        context: a valid AWS lambda Python context object.

    Returns:
        Dictionary with keys of table names whos values are booleans
        signifiying if the data was sucsessfully inserted into the
        Data Warehouse or not.
    """
    BUCKET = 'terrific-totes-processed-bucket-25'

    TABLE_LIST = ["dim_staff", "dim_date", "dim_location",
                  "dim_design", "dim_counterparty", "dim_currency"]

    results_dict = {}

    clear_fact_table()

    for table in TABLE_LIST:
        results_dict[f'{table}'] = False
        try:
            key = f'{table}.parquet'
            df = load_parquet_from_s3(BUCKET, key)
            rows = df.values.tolist()
            insert_data_into_db(rows, table)
        except Exception as err:
            logger.error(err)
        else:
            results_dict[f'{table}'] = True

    results_dict['fact_sales_order'] = False
    try:
        df = load_parquet_from_s3(BUCKET, "fact_sales_order.parquet")
        rows = df.values.tolist()
        insert_data_into_db(rows, 'fact_sales_order')
    except Exception as err:
        logger.error(err)
    else:
        results_dict['fact_sales_order'] = True
    finally:
        logger.info(results_dict)
        return results_dict


def get_secret_value(secret_name):
    """Finds data for a specified secret on SecretsManager.

    Args:
        secret_id: The Secret Name that holds the username and password
                    for your data base
    Returns:
        Dictionary containing data on secret
    Raises:
        ResourseNotFoundException
        ParamValidationError
        UnrecognizedClientException
        RuntimeError
    """
    secrets = boto3.client("secretsmanager")
    try:
        secret_value = secrets.get_secret_value(SecretId=secret_name)
    except secrets.exceptions.ResourceNotFoundException as e:
        logger.critical(f"The requested secret {secret_name} was not found")
        raise e
    except botocore.exceptions.ParamValidationError as e:
        logger.critical("The request has invalid params")
        raise e
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "UnrecognizedClientException":
            logger.critical("Security token invalid, check permissions")
            raise e
    except Exception as e:
        logger.critical(e)
        raise RuntimeError
    else:
        secrets_dict = json.loads(secret_value["SecretString"])
        return secrets_dict


def load_parquet_from_s3(bucket, key):
    """ Retrieve a Parquet file from an S3 bucket

    Args:
        bucket: Name of the S3 bucket from which to retrieve the file.
        key: Key that the file is stored under in the named S3 bucket.

    Returns:
        DataFrame containing the contents of the Parquet file.
    """
    s3 = boto3.client('s3')
    try:
        s3_response_object = s3.get_object(
            Bucket=bucket, Key=key)
        df = s3_response_object['Body'].read()
        df = pd.read_parquet(BytesIO(df))
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


def insert_data_into_db(data, table):
    """ Insert data into tables within the Data Warehouse DB

    Args:
        data: list of lists representing the rows to insert into the table
        table: string of the table name in which to insert data

    Returns:
        None
    """
    try:
        credentials = get_secret_value('warehouse_credentials')
        conn = get_warehouse_connection(credentials)
        cursor = conn.cursor()
    except Exception as err:
        logger.error(err)
    else:
        try:
            cursor.execute(f'DELETE FROM {table}')
            logger.info(f'Clearing data from table: {table}')
            query = f'INSERT INTO {table} VALUES %s'
            psycopg2.extras.execute_values(cursor, query, data)
            logger.info(f'Inserting data into table: {table}')
            conn.commit()
            logger.info(f'Changes commited to table: {table}')
        except Exception as err:
            logger.error(err)
        finally:
            cursor.close()
            conn.close()
            logger.info('Connection closed successfully')


def get_warehouse_connection(credentials):
    """ Establish connection to the Data Warehouse DB

    Args:
        credentials: Dictionary containing connection parameters
                     valid keys: "host", "dbname", "user", "password", "port"

    Returns:
        psycopg2 Connection object to the Data Warehouse DB
    """
    try:
        return psycopg2.connect(**credentials)
    except psycopg2.OperationalError:
        logger.error('Invalid Credentials.')
    except Exception as err:
        logger.error(err)


def clear_fact_table():
    """ Removes data from fact table

    Args:
        No arguments required

    Returns:
        None
    """
    try:
        credentials = get_secret_value('warehouse_credentials')
        conn = get_warehouse_connection(credentials)
        cursor = conn.cursor()
    except Exception as err:
        logger.error(err)
    else:
        try:
            cursor.execute('DELETE FROM fact_sales_order')
            logger.info('Clearing data from table: fact_sales_order')
            conn.commit()
            logger.info('Changes commited to table: fact_sales_order')
        except Exception as err:
            logger.error(err)
        finally:
            cursor.close()
            conn.close()
            logger.info('Connection closed successfully')
