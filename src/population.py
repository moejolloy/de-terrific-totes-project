import logging
import boto3
import json
import pandas as pd
from io import BytesIO
import psycopg2
import psycopg2.extras
import numpy as np
import os


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
        signifiying if the data was sucsessfully updated in the
        Data Warehouse or not.
    """
    BUCKET = os.environ.get('TF_PRO_BUCKET')

    TABLE_LIST = ["dim_staff", "dim_date", "dim_location",
                  "dim_design", "dim_counterparty", "dim_transaction",
                  "dim_payment_type", "dim_currency", "fact_sales_order",
                  "fact_purchase_order", "fact_payment"]

    results_dict = {}

    for table in TABLE_LIST:
        results_dict[f'{table}'] = False
        try:
            key = f'{table}.parquet'
            df = load_parquet_from_s3(BUCKET, key)
            insert_data_into_db(df, table)
        except Exception as err:
            logger.error(err)
        else:
            results_dict[f'{table}'] = True

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
    except Exception as e:
        logger.error(e)
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
    except Exception as e:
        logger.error(e)
        raise RuntimeError


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


def insert_data_into_db(data_df, table):
    """ Insert data into tables within the Data Warehouse DB

    Args:
        data: DataFrame representing the data to insert into the table
        table: string of the table name in which to insert data

    Returns:
        None
    """
    try:
        DEC2FLOAT = psycopg2.extensions.new_type(
            psycopg2.extensions.DECIMAL.values,
            'DEC2FLOAT',
            lambda value, curs: float(value) if value is not None else None)
        psycopg2.extensions.register_type(DEC2FLOAT)

        credentials = get_secret_value('warehouse_credentials')
        conn = get_warehouse_connection(credentials)
        cursor = conn.cursor()
    except Exception as err:
        logger.error(err)
        raise err
    else:
        try:
            KEY = {
                "dim_counterparty": "counterparty_id",
                "dim_currency": "currency_id",
                "dim_date": "date_id",
                "dim_design": "design_id",
                "dim_location": "location_id",
                "dim_payment_type": "payment_type_id",
                "dim_staff": "staff_id",
                "dim_transaction": "transaction_id",
                "fact_payment": "payment_record_id",
                "fact_purchase_order": "purchase_record_id",
                "fact_sales_order": "sales_record_id"
            }

            dw_query = f'SELECT * FROM {table};'
            cursor.execute(dw_query)
            dw_table = cursor.fetchall()
            dw_table_titles = [col[0] for col in cursor.description]
            dw_df = pd.DataFrame(dw_table, columns=dw_table_titles)

            TROUBLE_TABLES = {
                "dim_date": {
                    'date_id': 'datetime64[ns]'
                    },
                "dim_transaction": {
                    'sales_order_id': 'float64',
                    'purchase_order_id': 'float64'
                    },
                "fact_payment": {
                    'payment_date': 'datetime64[ns]'
                    },
                "fact_purchase_order": {
                    'agreed_delivery_date': 'datetime64[ns]',
                    'agreed_payment_date': 'datetime64[ns]'
                    },
                "fact_sales_order": {
                    'agreed_delivery_date': 'datetime64[ns]',
                    'agreed_payment_date': 'datetime64[ns]'
                    },
                }

            if table in list(TROUBLE_TABLES.keys()):
                if table == 'dim_transaction':
                    data_df = data_df.astype(TROUBLE_TABLES[table])
                else:
                    dw_df = dw_df.astype(TROUBLE_TABLES[table])

            print(data_df)
            print(dw_df)

            df_all = data_df.merge(
                dw_df, on=dw_table_titles, how='left', indicator=True)
            df_all = df_all.drop(df_all[df_all['_merge'] == 'both'].index)
            new_and_updated = df_all[df_all.columns[:-1]]

            if table == 'dim_transaction':
                new_and_updated = new_and_updated.astype(
                    {'sales_order_id': 'Int64', 'purchase_order_id': 'Int64'})
                new_and_updated = new_and_updated.replace({np.nan: None})

            print(new_and_updated)

            updated_rows = new_and_updated[
                new_and_updated[KEY[f'{table}']].isin(
                    dw_df[KEY[f'{table}']])].values.tolist()
            new_rows = new_and_updated[
                ~new_and_updated[KEY[f'{table}']].isin(
                    dw_df[KEY[f'{table}']])].values.tolist()

            logger.info(f'New rows to insert: {len(new_rows)}')
            if len(new_rows) > 0:
                new_query = f'INSERT INTO {table} VALUES %s;'
                psycopg2.extras.execute_values(cursor, new_query, new_rows)
                logger.info(f'Inserting new data into table: {table}')
                conn.commit()
                logger.info(f'New data commited to table: {table}')

            logger.info(f'Existing rows to update: {len(updated_rows)}')
            if len(updated_rows) > 0:
                update_cols = ', '.join(
                    [col + ' = data.' + col for col in dw_table_titles])
                update_query = f"UPDATE {table} SET {update_cols} \
                    FROM (VALUES %s) AS data ({', '.join(dw_table_titles)}) \
                        WHERE {table}.{KEY[f'{table}']} = \
                            data.{KEY[f'{table}']};"

                psycopg2.extras.execute_values(
                    cursor, update_query, updated_rows)
                logger.info(f'Updating existing data in table: {table}')
                conn.commit()
                logger.info(f'Updated data commited to table: {table}')

        except Exception as err:
            logger.error(err)
            raise err
        finally:
            cursor.close()
            conn.close()
            logger.info('Connection closed successfully')
