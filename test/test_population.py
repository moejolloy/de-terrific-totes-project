from src.population import (lambda_handler,
                            get_warehouse_connection,
                            insert_data_into_db)
from unittest.mock import patch
import pandas as pd
import logging
import psycopg2
import datetime


test_datetime = datetime.datetime.fromisoformat("2000-01-01T14:20:51.563000")
test_date = datetime.date.fromisoformat("2000-01-01")

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


@patch('src.population.psycopg2.extras')
@patch('src.population.get_warehouse_connection')
@patch('src.population.get_secret_value')
def test_insert_data_into_db(mock_gsv, mock_gwc, mock_extras, caplog):
    mock_gsv.return_value = {'user': 'name'}
    insert_data_into_db([[]], 'table1')
    assert caplog.records[0].levelno == logging.INFO
    assert caplog.records[0].msg == 'Clearing data from table: table1'
    assert caplog.records[1].levelno == logging.INFO
    assert caplog.records[1].msg == 'Inserting data into table: table1'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'Changes commited to table: table1'
    assert caplog.records[3].levelno == logging.INFO
    assert caplog.records[3].msg == 'Connection closed successfully'


@patch('src.population.get_secret_value')
def test_insert_data_into_db_credentials_error(mock_gsv, caplog):
    mock_gsv.side_effect = Exception('Error retrieving credentials')
    insert_data_into_db([[]], 'table1')
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'Error retrieving credentials'


@patch('src.population.get_warehouse_connection')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_connection_error(mock_gsv, mock_gwc, caplog):
    mock_gsv.return_value = {'user': 'name'}
    mock_gwc.side_effect = Exception('Connection error')
    insert_data_into_db([[]], 'table1')
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg.args[0] == 'Connection error'


@patch('src.population.psycopg2.extras')
@patch('src.population.get_warehouse_connection')
@patch('src.population.get_secret_value')
def test_insert_data_into_db_query_error(
        mock_gsv, mock_gwc, mock_extras, caplog):
    mock_gsv.return_value = {'user': 'name'}
    mock_extras.execute_values.side_effect = Exception('Error inserting data')
    insert_data_into_db([[]], 'table1')
    assert caplog.records[1].levelno == logging.ERROR
    assert caplog.records[1].msg.args[0] == 'Error inserting data'
    assert caplog.records[2].levelno == logging.INFO
    assert caplog.records[2].msg == 'Connection closed successfully'


@patch('src.population.load_parquet_from_s3')
@patch('src.population.insert_data_into_db')
def test_lambda_handler_returns_a_dictionary(mock_insert, mock_load):
    mock_load.side_effect = load_df
    test_result = {"dim_staff": True,
                   "dim_date": True,
                   "dim_location": True,
                   "dim_design": True,
                   "dim_counterparty": True,
                   "dim_currency": True,
                   "fact_sales_order": True,
                   "fact_purchase_order": True}
    assert lambda_handler({}, {}) == test_result


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


@patch('src.population.load_parquet_from_s3')
def test_lambda_handler_error(mock_load, caplog):
    mock_load.side_effect = Exception('Error loading file')
    lambda_handler({}, {})
    test_result = {"dim_staff": False,
                   "dim_date": False,
                   "dim_location": False,
                   "dim_design": False,
                   "dim_counterparty": False,
                   "dim_currency": False,
                   "fact_sales_order": False,
                   "fact_purchase_order": False}
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
    assert lambda_handler({}, {}) == test_result
