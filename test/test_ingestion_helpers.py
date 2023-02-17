import boto3
from moto import mock_secretsmanager
from unittest.mock import patch
import pytest
import os
import botocore.errorfactory
import botocore.exceptions as be
import logging

logger = logging.getLogger("TestLogger")


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


# Test SecretsManager
@pytest.fixture
def secretsmanager(aws_credentials):
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="us-east-1")


@pytest.fixture
def create_secret(secretsmanager):
    secretsmanager.create_secret(Name = 'MySecret', SecretString = '{"username":"username","password":"password"}')


def test_will_return_dictionary_containing_correct_values(secretsmanager, create_secret):
    from src.ingestion import get_secret_value

    secrets_dict = get_secret_value('MySecret')
    assert secrets_dict == {'password': 'password', 'username': 'username'} 


def test_will_throw_and_log_error_if_secret_name_not_found_in_secretsmanager(secretsmanager, create_secret, caplog):
    from src.ingestion import get_secret_value


    with pytest.raises(botocore.errorfactory.ClientError):
        get_secret_value('NotMySecret')

    assert caplog.records[0].levelno == logging.CRITICAL
    assert caplog.records[0].msg == 'The requested secret NotMySecret was not found'


def test_will_throw_and_log_error_if_secret_name_incorrect_type(secretsmanager, create_secret, caplog):
    from src.ingestion import get_secret_value


    with pytest.raises(be.ParamValidationError):
        get_secret_value(8)

    assert caplog.records[0].levelno == logging.CRITICAL
    assert caplog.records[0].msg == 'The request has invalid params'


def test_logging_all_other_errors(caplog):
    import src.ingestion

    with patch('src.ingestion.secrets.get_secret_value') as mock:
        response_error = Exception
        mock.side_effect = response_error

        with pytest.raises(RuntimeError):
            src.ingestion.get_secret_value('MySecret')

        assert caplog.records[0].levelno == logging.CRITICAL


# Test SQL Helpers
def test_get_keys_from_table_names_applies_correct_suffix():
    from src.ingestion import get_keys_from_table_names
    
    
    result = get_keys_from_table_names(['table_1', 'table_2', 'table_3'])
    assert result == ['table_1.csv', 'table_2.csv', 'table_3.csv']
