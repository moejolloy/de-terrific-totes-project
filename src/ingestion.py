import pg8000
import pg8000.native as pg
import boto3
import csv
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger('IngestionLogger')
logger.setLevel(logging.INFO)

HOST = (f'nc-data-eng-totesys-production.chpsczt8h1nu.'
        f'eu-west-2.rds.amazonaws.com')
PORT = 5432
USER = 'project_user_4'
PASS = 'ZUr7UMAkA3mPQgrQ2jckFDfa'
DATABASE = 'totesys'

def get_connection(user, password, database, host, port=5432):
    try:
        conn = pg.Connection(
                            user, 
                            host = host, 
                            port = port , 
                            password = password, 
                            database = database
                        )
        return conn
    except pg8000.exceptions.DatabaseError:
        logger.error('Unable to connect to database')
        raise Exception('Unable to connect to database')


def data_table_to_csv(conn, query: str, file_path: str, columns: list) -> None:
    try:
        result = conn.run(query)
        with open(file_path, 'w') as file:
            writer = csv.writer(file)
            writer.writerow(columns)
            writer.writerows(result)
    except ClientError as e:
        logging.error(e)
        return False
    return True

staff_columns = [
                'staff_id', 
                'first_name', 
                'last_name', 
                'department_id', 
                'email_address', 
                'created_at', 
                'last_updated'
                ]

transaction_columns = [
                        'transaction_id', 
                        'transaction_type', 
                        'sales_order_id', 
                        'purchase_order_id', 
                        'created_at', 
                        'last_updated', 
                        ]

design_columns = [
                'design_id', 
                'created_at', 
                'last_updated', 
                'design_name', 
                'file_location', 
                'file_name'
                ]

address_columns = [
                'address_id', 
                'address_line_1', 
                'address_line_2', 
                'district', 
                'city', 
                'postal_code', 
                'country', 
                'phone', 
                'created_at', 
                'last_updated'
                ]

sales_columns = ['sales_order_id', 
                'created_at', 
                'last_updated', 
                'design_id', 
                'staff_id',
                'counterparty_id', 
                'units_sold', 
                'unit_price', 
                'currency_id', 
                'agreed_delivery_date', 
                'agreed_payment_date', 
                'agreed_delivery_location_id']

conn = get_connection(USER, PASS, DATABASE, HOST, PORT)
data_table_to_csv(conn, 'SELECT * FROM staff;', './raw-data/staff.csv', staff_columns)

s3 = boto3.client('s3')
def create_bucket():
        s3.create_bucket(Bucket='test-bucket-00000009988')
def move_csv_to_bucket(bucket_name, key):
    try:
        with open('./raw-data/staff.csv', 'rb') as file:
            s3.put_object(Bucket=bucket_name, Key='staff.csv', Body=file)
            print('csv added to bucket')
    except ClientError as e:
        logging.error(e)
        return False
    return True
