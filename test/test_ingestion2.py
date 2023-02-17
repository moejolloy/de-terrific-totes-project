from unittest.mock import patch, Mock
import src.conn
import pytest

# @pytest.fixture
# def mock_pg():
#     with patch('src.conn.pg8000.native') as mock:
#         yield mock

@patch('src.conn.get_connection', return_value = Mock())
def test_mock_connection(mock_connection):
    
    # with patch('src.conn.Connection.run') as mock:
    #     mock().return_value = []
    mock_con = Mock()
    mock_connection.run = []
    mock_connection.columns = [{'name':'1'}, {'name':'2'}]
    assert src.conn.get_connection.run.return_value == []
    assert src.conn.get_connection.columns == [{'name':'1'}, {'name':'2'}]
    assert src.conn.sql_select_column_headers(['column_1', 'column_2'], src.conn.get_connection) == []

# from src.conn import sql_select_column_headers
# @patch('src.conn.get_connection')
# def test2(mock_db):
#     mock_db.return_value.

# def test_mock_connection():
#     with patch('src.conn.Connection', return_value = Mock()) as mock_db:
#         mock_db.run.return_value = []
        
#     assert src.conn.sql_select_column_headers(['column_1', 'column_2']) == []