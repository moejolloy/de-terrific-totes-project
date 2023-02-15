import pg8000
import pg8000.native as pg
import logging
from botocore.exceptions import ClientError
from io import StringIO

#import psycopg2
#import request
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

staff_columns=['staff_id','first_name','last_name','department_id','email_address','created_at','last_updated']
database_to_bucket_csv_file('staff', staff_columns, 'pandas-351803', 'df.csv')





