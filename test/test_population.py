from src.population import (lambda_handler,
                            get_warehouse_connection,
                            insert_data_into_db)
from unittest.mock import patch
import pandas as pd
import logging
import psycopg2

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

