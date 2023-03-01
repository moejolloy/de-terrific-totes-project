from src.population import (lambda_handler,
                            get_warehouse_connection,
                            insert_data_into_db)
from unittest.mock import patch
import pandas as pd
import logging
import psycopg2
import datetime
import pytest
import botocore.errorfactory
import botocore.exceptions
from botocore.response import StreamingBody
import io


test_datetime = datetime.datetime.fromisoformat("2000-01-01T14:20:51.563000")
test_date = datetime.date.fromisoformat("2000-01-01")
test_time = datetime.time.fromisoformat("14:20:51.563000")

logger = logging.getLogger("TestLogger")


@patch('src.population.psycopg2.extensions.connection')
@patch('src.population.psycopg2.connect')
def test_get_warehouse_connection_return_value(mock_conn, mock_class):
    mock_class.return_value = {"Type": "Connection Class"}
    mock_conn.return_value = psycopg2.extensions.connection()
    assert get_warehouse_connection({}) == {"Type": "Connection Class"}


@patch('src.population.psycopg2.connect')
def test_get_warehouse_connection_error_logging(mock_conn, caplog):
    mock_conn.side_effect = Exception('An error occured.')
    get_warehouse_connection({})
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'An error occured.'


@patch('src.population.psycopg2.connect')
def test_get_warehouse_connection_operational_error_logging(mock_conn, caplog):
    mock_conn.side_effect = psycopg2.OperationalError
    get_warehouse_connection({"dbname": "TestDB"})
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == 'Invalid Credentials.'


@patch('src.population.psycopg2.connect')
@patch('src.population.psycopg2.extras')
@patch('src.population.get_secret_value')
def test_insert_data_into_db(mock_gsv, mock_extras, mock_connect, caplog):
    staff_data = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Joe", "Max"],
        "last_name": ["Ant", "Boo", "Car"],
        "department_id": [1, 2, 3],
        "location": ['Loc1', 'Loc2', 'Loc3'],
        "email_address": ["a@b.com", "b@c.com", "c@d.com"],
    }
    df = pd.DataFrame(data=staff_data)
    mock_gsv.return_value = {'user': 'name'}
    mock_connect.return_value.cursor.return_value.fetchall.return_value \
        = df.values.tolist()
    mock_connect.return_value.cursor.return_value.description = [
        ['staff_id'],
        ['first_name'],
        ['last_name'],
        ['department_id'],
        ['location'],
        ['email_address']]
    insert_data_into_db(df, 'dim_staff')
    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == 'New rows to insert: 0'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Existing rows to update: 0'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'Connection closed successfully'


@patch('src.population.psycopg2.connect')
@patch('src.population.psycopg2.extras')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_dim_transaction(mock_gsv, mock_extras,
                                             mock_connect, caplog):
    transaction_data = {
            "transaction_id": [1, 2, 3],
            "transaction_type": ["type1", "type2", "type3"],
            "sales_order_id": [1, 2, 3],
            "purchase_order_id": [1, 2, 3]
            }
    df = pd.DataFrame(data=transaction_data)
    mock_gsv.return_value = {'user': 'name'}
    mock_connect.return_value.cursor.return_value.fetchall.return_value \
        = df.values.tolist()
    mock_connect.return_value.cursor.return_value.description = [
            ["transaction_id"],
            ["transaction_type"],
            ["sales_order_id"],
            ["purchase_order_id"]]
    insert_data_into_db(df, 'dim_transaction')
    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == 'New rows to insert: 0'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Existing rows to update: 0'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'Connection closed successfully'


@patch('src.population.psycopg2.connect')
@patch('src.population.psycopg2.extras')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_fact_sales_order(mock_gsv, mock_extras,
                                              mock_connect, caplog):
    sales_data = {
        "sales_order_id": [1, 2, 3],
        "staff_id": [1, 2, 3],
        "created_date": [test_date, test_date, test_date],
        "created_time": [test_time, test_time, test_time],
        "last_updated_date": [test_date, test_date, test_date],
        "last_updated_time": [test_time, test_time, test_time],
        "counterparty_id": [1, 2, 3],
        "units_sold": [100, 200, 300],
        "unit_price": [2.45, 3.67, 9.87],
        "currency_id": [1, 2, 3],
        "design_id": ["des1", "des2", "des3"],
        "agreed_payment_date": [test_date, test_date, test_date],
        "agreed_delivery_date": [test_date, test_date, test_date],
        "agreed_delivery_location_id": [1, 2, 3]
            }
    df = pd.DataFrame(data=sales_data)
    TROUBLE_TABLES = {"fact_sales_order": {
        'agreed_delivery_date': 'datetime64[ns]',
        'agreed_payment_date': 'datetime64[ns]'
                    }
                }
    df = df.astype(TROUBLE_TABLES['fact_sales_order'])
    mock_gsv.return_value = {'user': 'name'}
    mock_connect.return_value.cursor.return_value.fetchall.return_value \
        = df.values.tolist()
    mock_connect.return_value.cursor.return_value.description = [
        ["sales_order_id"],
        ["staff_id"],
        ["created_date"],
        ["created_time"],
        ["last_updated_date"],
        ["last_updated_time"],
        ["counterparty_id"],
        ["units_sold"],
        ["unit_price"],
        ["currency_id"],
        ["design_id"],
        ["agreed_payment_date"],
        ["agreed_delivery_date"],
        ["agreed_delivery_location_id"]]
    insert_data_into_db(df, 'fact_sales_order')
    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == 'New rows to insert: 0'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Existing rows to update: 0'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'Connection closed successfully'


@patch('src.population.psycopg2.connect')
@patch('src.population.psycopg2.extras')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_new_rows(mock_gsv, mock_extras,
                                      mock_connect, caplog):
    staff_data_db = {
        "staff_id": [1, 2, 3, 4],
        "first_name": ["Sam", "Joe", "Max", "Bob"],
        "last_name": ["Ant", "Boo", "Car", "Dee"],
        "department_id": [1, 2, 3, 4],
        "location": ['Loc1', 'Loc2', 'Loc3', 'Loc4'],
        "email_address": ["a@b.com", "b@c.com", "c@d.com", "d@e.com"],
    }
    df_db = pd.DataFrame(data=staff_data_db)
    staff_data_dw = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Joe", "Max"],
        "last_name": ["Ant", "Boo", "Car"],
        "department_id": [1, 2, 3],
        "location": ['Loc1', 'Loc2', 'Loc3'],
        "email_address": ["a@b.com", "b@c.com", "c@d.com"],
    }
    df_dw = pd.DataFrame(data=staff_data_dw)
    mock_gsv.return_value = {'user': 'name'}
    mock_connect.return_value.cursor.return_value.fetchall.return_value \
        = df_dw.values.tolist()
    mock_connect.return_value.cursor.return_value.description = [
        ['staff_id'],
        ['first_name'],
        ['last_name'],
        ['department_id'],
        ['location'],
        ['email_address']]
    insert_data_into_db(df_db, 'dim_staff')
    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == 'New rows to insert: 1'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Inserting new data into table: dim_staff'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'New data commited to table: dim_staff'
    assert caplog.records[3].levelno == logging.INFO
    assert caplog.records[3].msg == 'Existing rows to update: 0'
    assert caplog.records[4].levelno == logging.INFO
    assert caplog.records[4].msg == 'Connection closed successfully'


@patch('src.population.psycopg2.connect')
@patch('src.population.psycopg2.extras')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_update_rows(mock_gsv, mock_extras,
                                         mock_connect, caplog):
    staff_data_db = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Dave", "Max"],
        "last_name": ["Ant", "Smith", "Car"],
        "department_id": [1, 4, 3],
        "location": ['Loc1', 'Loc4', 'Loc3'],
        "email_address": ["a@b.com", "d@e.com", "c@d.com"],
    }
    df_db = pd.DataFrame(data=staff_data_db)
    staff_data_dw = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Joe", "Max"],
        "last_name": ["Ant", "Boo", "Car"],
        "department_id": [1, 2, 3],
        "location": ['Loc1', 'Loc2', 'Loc3'],
        "email_address": ["a@b.com", "b@c.com", "c@d.com"],
    }
    df_dw = pd.DataFrame(data=staff_data_dw)
    mock_gsv.return_value = {'user': 'name'}
    mock_connect.return_value.cursor.return_value.fetchall.return_value \
        = df_dw.values.tolist()
    mock_connect.return_value.cursor.return_value.description = [
        ['staff_id'],
        ['first_name'],
        ['last_name'],
        ['department_id'],
        ['location'],
        ['email_address']]
    insert_data_into_db(df_db, 'dim_staff')
    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == 'New rows to insert: 0'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Existing rows to update: 1'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'Updating existing data in table: '\
        'dim_staff'
    assert caplog.records[3].levelno == logging.INFO
    assert caplog.records[3].msg == 'Updated data commited to table: dim_staff'
    assert caplog.records[4].levelno == logging.INFO
    assert caplog.records[4].msg == 'Connection closed successfully'


@patch('src.population.get_secret_value')
def test_insert_data_into_db_credentials_error(mock_gsv, caplog):
    mock_gsv.side_effect = Exception('Error retrieving credentials')
    df = pd.DataFrame([[1, 2], [3, 4]])
    with pytest.raises(Exception):
        insert_data_into_db(df, 'table1')
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'Error retrieving credentials'


@patch('src.population.get_warehouse_connection')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_connection_error(mock_gsv, mock_gwc, caplog):
    mock_gsv.return_value = {'user': 'name'}
    mock_gwc.side_effect = Exception('Connection error')
    df = pd.DataFrame([[1, 2], [3, 4]])
    with pytest.raises(Exception):
        insert_data_into_db(df, 'table1')
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'Connection error'


@patch('src.population.psycopg2.connect')
@patch('src.population.psycopg2.extras')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_query_error(
        mock_gsv, mock_extras, mock_connect, caplog):
    staff_data_db = {
        "staff_id": [1, 2, 3, 4],
        "first_name": ["Sam", "Joe", "Max", "Bob"],
        "last_name": ["Ant", "Boo", "Car", "Dee"],
        "department_id": [1, 2, 3, 4],
        "location": ['Loc1', 'Loc2', 'Loc3', 'Loc4'],
        "email_address": ["a@b.com", "b@c.com", "c@d.com", 'd@e.com'],
    }
    staff_data_dw = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Joe", "Max"],
        "last_name": ["Ant", "Boo", "Car"],
        "department_id": [1, 2, 3],
        "location": ['Loc1', 'Loc2', 'Loc3'],
        "email_address": ["a@b.com", "b@c.com", "c@d.com"],
    }
    db_df = pd.DataFrame(data=staff_data_db)
    dw_df = pd.DataFrame(data=staff_data_dw)
    mock_connect.return_value.cursor.return_value.fetchall.return_value \
        = dw_df.values.tolist()
    mock_connect.return_value.cursor.return_value.description = [
        ['staff_id'],
        ['first_name'],
        ['last_name'],
        ['department_id'],
        ['location'],
        ['email_address']]
    mock_gsv.return_value = {'user': 'name'}
    mock_extras.execute_values.side_effect = Exception('Error inserting data')
    with pytest.raises(Exception):
        insert_data_into_db(db_df, 'table1')
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'table1'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Connection closed successfully'


@patch('src.population.load_parquet_from_s3')
@patch('src.population.insert_data_into_db')
def test_lambda_handler_returns_a_dictionary(mock_insert, mock_load):
    mock_load.side_effect = load_df
    test_result = {"dim_staff": True,
                   "dim_date": True,
                   "dim_location": True,
                   "dim_design": True,
                   "dim_counterparty": True,
                   "dim_transaction": True,
                   "dim_payment_type": True,
                   "dim_currency": True,
                   "fact_sales_order": True,
                   "fact_purchase_order": True,
                   "fact_payment": True}
    assert lambda_handler({}, {}) == test_result


@patch('src.population.load_parquet_from_s3')
def test_lambda_handler_error(mock_load, caplog):
    mock_load.side_effect = Exception('Error loading file')
    lambda_handler({}, {})
    test_result = {"dim_staff": False,
                   "dim_date": False,
                   "dim_location": False,
                   "dim_design": False,
                   "dim_counterparty": False,
                   "dim_transaction": False,
                   "dim_payment_type": False,
                   "dim_currency": False,
                   "fact_sales_order": False,
                   "fact_purchase_order": False,
                   "fact_payment": False}
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'Error loading file'
    assert caplog.records[1].levelno == logging.ERROR
    assert caplog.records[1].msg.args[0] == 'Error loading file'
    assert caplog.records[2].levelno == logging.ERROR
    assert caplog.records[2].msg.args[0] == 'Error loading file'
    assert caplog.records[3].levelno == logging.ERROR
    assert caplog.records[3].msg.args[0] == 'Error loading file'
    assert caplog.records[4].levelno == logging.ERROR
    assert caplog.records[4].msg.args[0] == 'Error loading file'
    assert caplog.records[5].levelno == logging.ERROR
    assert caplog.records[5].msg.args[0] == 'Error loading file'
    assert caplog.records[6].levelno == logging.ERROR
    assert caplog.records[6].msg.args[0] == 'Error loading file'
    assert caplog.records[7].levelno == logging.ERROR
    assert caplog.records[7].msg.args[0] == 'Error loading file'
    assert caplog.records[8].levelno == logging.ERROR
    assert caplog.records[8].msg.args[0] == 'Error loading file'
    assert caplog.records[9].levelno == logging.ERROR
    assert caplog.records[9].msg.args[0] == 'Error loading file'
    assert caplog.records[10].levelno == logging.ERROR
    assert caplog.records[10].msg.args[0] == 'Error loading file'
    assert lambda_handler({}, {}) == test_result


@patch('src.population.boto3.client')
def test_get_secret_value(mock_client):
    from src.population import get_secret_value
    text = {'SecretString': '{"Test":"Test"}'}
    mock_client.return_value.get_secret_value.return_value = text
    assert get_secret_value('Test') == {"Test": "Test"}


@patch('src.population.boto3.client')
def test_get_secret_value_error(mock_client):
    from src.population import get_secret_value
    err = botocore.errorfactory.ClientError(
        {'Error': {"Code": "UnrecognizedClientException"}}, '')
    err.response["Error"]["Code"] == "UnrecognizedClientException"
    mock_client.return_value.get_secret_value.side_effect = err
    with pytest.raises(RuntimeError):
        get_secret_value('test')


@patch('src.population.pd.read_parquet')
@patch('src.population.boto3.client')
def test_load_parquet_from_s3(mock_client, mock_parq):
    from src.population import load_parquet_from_s3
    read_file = b'ID\n1\n2'
    body = StreamingBody(io.BytesIO(read_file), len(read_file))
    response = {'Body': body}
    mock_client.return_value.get_object.return_value = response
    data = {"ID": [1, 2]}
    df = pd.DataFrame(data=data)
    mock_parq.return_value = df
    assert load_parquet_from_s3("", "").equals(df)


@patch('src.population.boto3.client')
def test_load_parquet_from_s3_error(mock_client):
    from src.population import load_parquet_from_s3
    err = botocore.errorfactory.ClientError(
        {'Error': {"Code": "UnrecognizedClientException"}}, '')
    err.response["Error"]["Code"] == "UnrecognizedClientException"
    mock_client.return_value.get_object.side_effect = err
    with pytest.raises(RuntimeError):
        load_parquet_from_s3('test', 'key')


def load_df(bucket, key, parse_dates=[]):
    if key == "dim_staff.parquet":
        staff_data = {
            "staff_id": [1, 2, 3],
            "first_name": ["Sam", "Joe", "Max"],
            "last_name": ["Ant", "Boo", "Car"],
            "department_id": [1, 2, 3],
            "email_address": ["a@b.com", "b@c.com", "c@d.com"],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=staff_data)
    elif key == "dim_date.parquet":
        dept_data = {
            "department_id": [1, 2, 3],
            "department_name": ["a", "b", "c"],
            "location": ["loca", "locb", "locc"],
            "manager": ["mrs a", "mr b", "ms c"],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=dept_data)
    elif key == "dim_counterparty.parquet":
        counterparty_data = {
            'counterparty_id': [1, 2, 3],
            'counterparty_legal_name': ['NameA', 'NameB', 'NameC'],
            'legal_address_id': [1, 2, 3],
            'commercial_contact': ['PersonA', 'PersonB', 'PersonC'],
            'delivery_contact': ['PersonD', 'PersonE', 'PersonF'],
            'created_at': [
                test_datetime,
                test_datetime,
                test_datetime],
            'last_updated': [
                test_datetime,
                test_datetime,
                test_datetime]
        }
        return pd.DataFrame(data=counterparty_data)
    elif key == "dim_transaction.parquet":
        transaction_data = {
            "transaction_id": [1, 2, 3],
            "transaction_type": ["type1", "type2", "type3"],
            "sales_order_id": [1, 2, 3],
            "purchase_order_id": [1, 2, 3],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=transaction_data)
    elif key == "dim_payment_type.parquet":
        payment_type_data = {
            "payment_type_id": [1, 2, 3],
            "payment_type_name": ["name1", "name2", "name3"],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=payment_type_data)
    elif key == "dim_currency.parquet":
        currency_data = {
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR'],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=currency_data)
    elif key == "dim_location.parquet":
        location_data = {
            "address_id": [1, 2, 3],
            "address_line_1": ["17", "23", "44"],
            "address_line_2": ["Green Street", "Yellow Road", None],
            "district": ["districta", "districtb", "districtc"],
            "city": ["citya", "cityb", "cityc"],
            "postal_code": ["123", "ABC", "987"],
            "country": ["Uk", "USA", "Ireland"],
            "phone": [123, 456, 789],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=location_data)
    elif key == "dim_design.parquet":
        design_data = {
            "design_id": [1, 2, 3],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "design_name": ["design_c", "design_b", "design_a"],
            "file_location": ["loc_a", "loc_b", "loc/c"],
            "file_name": ["filename_a", "filename_b", "filename_c"],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=design_data)
    elif key == "fact_sales_order.parquet":
        sales_order_data = {
            "sales_order_id": [1, 2, 3],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime],
            "design_id": ["des1", "des2", "des3"],
            "staff_id": [1, 2, 3],
            "counterparty_id": [1, 2, 3],
            "units_sold": [100, 200, 300],
            "unit_price": [2.45, 3.67, 9.87],
            "currency_id": [1, 2, 3],
            "agreed_delivery_date": [test_date, test_date, test_date],
            "agreed_payment_date": [test_date, test_date, test_date],
            "agreed_delivery_location_id": [1, 2, 3]
        }
        return pd.DataFrame(data=sales_order_data)
    elif key == "fact_purchase_order.parquet":
        purchase_order_data = {
            "purchase_order_id": [1, 2, 3],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime],
            "staff_id": [1, 2, 3],
            "counterparty_id": [1, 2, 3],
            "item_code": ["dummy data 1", "dummy data 2", "dummy data 3"],
            "design_id": ["des1", "des2", "des3"],
            "item_quantity": [3, 4, 9],
            "item_unit_price": [4, 8, 3],
            "currency_id": [1, 2, 3],
            "agreed_delivery_date": [test_date, test_date, test_date],
            "agreed_payment_date": [test_date, test_date, test_date],
            "agreed_delivery_location_id": [1, 2, 3]
        }
        return pd.DataFrame(data=purchase_order_data)
    elif key == "fact_payment.parquet":
        payment_data = {
            "payment_id": [1, 2, 3],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime],
            "transaction_id": [1, 2, 3],
            "counterparty_id": [1, 2, 3],
            "payment_amount": [3, 7, 9],
            "currency_id": [1, 2, 3],
            "payment_type_id": ["type1", "type2", "type3"],
            "paid": [True, False, True],
            "payment_date": [test_date, test_date, test_date],
            "company_ac_number": [1, 2, 3],
            "counterparty_ac_number": [1, 2, 3]
        }
        return pd.DataFrame(data=payment_data)
