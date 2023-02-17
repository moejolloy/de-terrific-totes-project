from pg8000.native import Connection


# HOST = (f'nc-data-eng-totesys-production.chpsczt8h1nu.'
#         f'eu-west-2.rds.amazonaws.com')
# PORT = 5432
# USER = 'username'
# PASS = 'password'
# DATABASE = 'totesys'

def get_connection():
    return Connection(user = '', password = '', database = '', host = '', port = '')

def sql_select_column_headers(table, conn):
    # conn = get_connection()
    conn.run(f'SELECT * FROM {table};')
    return ([column['name'] for column in conn.columns])
    