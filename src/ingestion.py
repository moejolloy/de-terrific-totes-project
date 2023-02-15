import pg8000
import pg8000.native as pg
import logging
from botocore.exceptions import ClientError
from io import StringIO

import boto3
import pandas as pd
from pandas import Series, DataFrame
import csv


HOST = (f'nc-data-eng-totesys-production.chpsczt8h1nu.'
        f'eu-west-2.rds.amazonaws.com')
PORT = 5432
USER = 'project_user_4'
PASS = 'ZUr7UMAkA3mPQgrQ2jckFDfa'
DATABASE = 'totesys'

s3_resource = boto3.resource('s3')

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

conn = get_connection(USER, PASS, DATABASE, HOST, PORT)

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

sales_order_columns = [
                'sales_order_id', 
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
                'agreed_delivery_location_id'
                ]

counterparty_columns = [
                'counterparty_id'
                'counterparty_legal_name', 
                'legal_address_id', 
                'commercial_contact', 
                'delivery_contact', 
                'created_at',  
                'last_updated'
                ]

payment_columns = [
    'payment_id',
    'created_at',
    'last_updated',
    'transaction_id',
    'counterparty_id',
    'payment_amount',
    'currency_id',
    'payment_type_id',
    'paid',
    'payment_date',
    'company_ac_number',
    'counterparty_ac_number'
]

payment_type_columns = [
    'payment_type_id',
    'payment_type_name',
    'created-at',
    'last_updated'

]

currency_columns = [
    'currency_id',
    'currency_code',
    'created_at',
    'last_updated'
]

department_columns = [
    'departmemt_id',
    'department_name',
    'location',
    'manager',
    'created-at',
    'last_updated'
]

purchase_order_columns = [
    'purchase_order_id',
    'created_at',
    'last_updated',
    'staff_id',
    'counterparty_id',
    'item_code',
    'item_quantity',
    'item_unit_price',
    'currency_id',
    'agreed_delivery_date'
    'agreed_payment_date',
    'agreed_delivery_location_id'
]







def database_to_bucket_csv_file(table_name, column_headers, bucket_name, bucket_key):
    myresult = conn.run(f'SELECT * FROM {table_name};')
    item_list = []
    for i in myresult:
        item = {}
        for index, column in enumerate(column_headers):
            item[column] = i[index]
        item_list.append(item)
    df = pd.DataFrame(data=item_list,columns=column_headers)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket_name, bucket_key).put(Body=csv_buffer.getvalue())


database_to_bucket_csv_file('staff', staff_columns, 'pandas-351803', 'staff.csv')
database_to_bucket_csv_file('transaction', transaction_columns, 'pandas-351803', 'transaction.csv')
database_to_bucket_csv_file('design', design_columns, 'pandas-351803', 'design.csv')
database_to_bucket_csv_file('address', address_columns, 'pandas-351803', 'address.csv')
database_to_bucket_csv_file('sales_order', sales_order_columns, 'pandas-351803', 'sales_orders.csv')
database_to_bucket_csv_file('counterparty', counterparty_columns, 'pandas-351803', 'counterparty.csv')
database_to_bucket_csv_file('payment', payment_columns, 'pandas-351803', 'payment.csv')
database_to_bucket_csv_file('payment_type', payment_type_columns, 'pandas-351803', 'payment_type.csv')
database_to_bucket_csv_file('currency', currency_columns, 'pandas-351803', 'currency.csv')
database_to_bucket_csv_file('department', department_columns, 'pandas-351803', 'department.csv')
database_to_bucket_csv_file('purchase_order', purchase_order_columns, 'pandas-351803', 'purchase_order.csv')