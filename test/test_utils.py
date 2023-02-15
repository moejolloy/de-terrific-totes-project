from unittest.mock import Mock, patch
from botocore.response import StreamingBody
import src.utils
import boto3
import io
import pandas as pd


def test_load_from_s3_reads_csv_file_from_s3_bucket_and_returns_dataframe():
    with patch('src.utils.s3.get_object') as mock:
        read_file = b'ID\n1\n2'
        body = StreamingBody(io.BytesIO(read_file), len(read_file))
        response = {'Body': body}
        mock.return_value = response
        data = {"ID": [1, 2]}
        df = pd.DataFrame(data=data)
        assert src.utils.load_from_s3("", "").equals(df)


def test_load_from_s3_reads_more_complex_csv_file_from_s3_bucket_and_returns_dataframe():
    with patch('src.utils.s3.get_object') as mock:
        read_file = b'ID,Name,Age\n1,Sam,31\n2,Joe,14\n3,Max,44'
        body = StreamingBody(io.BytesIO(read_file), len(read_file))
        response = {'Body': body}
        mock.return_value = response
        data = {"ID": [1, 2, 3], "Name": [
            "Sam", "Joe", "Max"], "Age": [31, 14, 44]}
        df = pd.DataFrame(data=data)
        assert src.utils.load_from_s3("", "").equals(df)

# mock errors

# def test_reads_file_from_bucket_and_prints_to_console():
#     with patch('src.warm_up.s3_client') as mock:
#         read_file = b'hello'
#         body = StreamingBody(io.BytesIO(read_file), len(read_file))
#         response = {'Body': body}
#         mock.get_object.return_value = response
#         assert src.warm_up.read_file(
#             'zen.txt') == 'hello'
