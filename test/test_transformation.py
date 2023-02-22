import pandas as pd
from unittest.mock import patch
from src.transformation import (format_dim_staff,
                                format_dim_location,
                                format_dim_design,
                                format_dim_date,
                                format_dim_currency,
                                format_dim_counterparty,
                                format_fact_sales_order,
                                transform_data)
import datetime
import logging

logger = logging.getLogger("testing_transform")
logger.setLevel(logging.INFO)

test_datetime = datetime.datetime.fromisoformat("2000-01-01T14:20:51.563000")
test_date = datetime.date.fromisoformat("2000-01-01")
test_td = datetime.timedelta(days=1)
test_time = datetime.time.fromisoformat("14:20:51.563000")


def test_format_dim_staff_returns_expected_output():
    staff_data = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Joe", "Max"],
        "last_name": ["Ant", "Boo", "Car"],
        "department_id": [1, 2, 3],
        "email_address": ["a@b.com", "b@c.com", "c@d.com"],
        "created_at": [test_datetime, test_datetime, test_datetime],
        "last_updated": [test_datetime, test_datetime, test_datetime]
    }
    staff_df = pd.DataFrame(data=staff_data)
    dept_data = {
        "department_id": [1, 2, 3],
        "department_name": ["a", "b", "c"],
        "location": ["loca", "locb", "locc"],
        "manager": ["mrs a", "mr b", "ms c"],
        "created_at": [test_datetime, test_datetime, test_datetime],
        "last_updated": [test_datetime, test_datetime, test_datetime]
    }
    dept_df = pd.DataFrame(data=dept_data)

    test_dim_staff_data = {
        "staff_id": [1, 2, 3],
        "first_name": ["Sam", "Joe", "Max"],
        "last_name": ["Ant", "Boo", "Car"],
        "department_name": ["a", "b", "c"],
        "location": ["loca", "locb", "locc"],
        "email_address": ["a@b.com", "b@c.com", "c@d.com"]
    }
    dim_staff_df = pd.DataFrame(data=test_dim_staff_data)
    assert format_dim_staff(staff_df, dept_df).equals(dim_staff_df)


def test_format_dim_location_returns_expected_output():
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
    location_df = pd.DataFrame(data=location_data)

    test_dim_location_data = {
        "location_id": [1, 2, 3],
        "address_line_1": ["17", "23", "44"],
        "address_line_2": ["Green Street", "Yellow Road", None],
        "district": ["districta", "districtb", "districtc"],
        "city": ["citya", "cityb", "cityc"],
        "postal_code": ["123", "ABC", "987"],
        "country": ["Uk", "USA", "Ireland"],
        "phone": [123, 456, 789]
    }
    dim_location_df = pd.DataFrame(data=test_dim_location_data)
    assert format_dim_location(location_df).equals(dim_location_df)


def test_format_dim_design_returns_expected_output():
    design_data = {
        "design_id": [1, 2, 3],
        "created_at": [test_datetime, test_datetime, test_datetime],
        "design_name": ["design_c", "design_b", "design_a"],
        "file_location": ["loc_a", "loc_b", "loc/c"],
        "file_name": ["filename_a", "filename_b", "filename_c"],
        "last_updated": [test_datetime, test_datetime, test_datetime]
    }
    design_df = pd.DataFrame(data=design_data)

    test_dim_design_data = {
        "design_id": [1, 2, 3],
        "design_name": ["design_c", "design_b", "design_a"],
        "file_location": ["loc_a", "loc_b", "loc/c"],
        "file_name": ["filename_a", "filename_b", "filename_c"]
    }
    dim_design_df = pd.DataFrame(data=test_dim_design_data)
    assert format_dim_design(design_df).equals(dim_design_df)


def test_format_dim_date_returns_expected_output():
    start = '2000-01-01'
    end = '2000-01-10'

    test_date = datetime.datetime.fromisoformat("2000-01-01")

    test_dim_date_data = {
        "date_id": [test_date, test_date + test_td, test_date + (test_td * 2),
                    test_date + (test_td * 3), test_date + (test_td * 4),
                    test_date + (test_td * 5), test_date + (test_td * 6),
                    test_date + (test_td * 7), test_date + (test_td * 8),
                    test_date + (test_td * 9)],
        "year": [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000],
        "month": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
        "day": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "day_of_week": [5, 6, 0, 1, 2, 3, 4, 5, 6, 0],
        "day_name": ['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday',
                     'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday'],
        "month_name": ['January', 'January', 'January', 'January', 'January',
                       'January', 'January', 'January', 'January', 'January'],
        "quarter": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    }

    dim_date_df = pd.DataFrame(data=test_dim_date_data)
    assert format_dim_date(start, end).equals(dim_date_df)


def test_format_dim_currency_returns_expected_output():
    currency_data = {
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        "created_at": [test_datetime, test_datetime, test_datetime],
        "last_updated": [test_datetime, test_datetime, test_datetime]
    }
    currency_df = pd.DataFrame(data=currency_data)

    test_dim_currency_data = {
        'currency_id': [1, 2, 3],
        'currency_code': ['GBP', 'USD', 'EUR'],
        'currency_name': ['British Pound sterling',
                          'United States dollar',
                          'Euro']
    }
    dim_currency_df = pd.DataFrame(data=test_dim_currency_data)

    assert format_dim_currency(currency_df).equals(dim_currency_df)


def test_format_dim_counterparty_returns_expected_output():
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
    counterparty_df = pd.DataFrame(data=counterparty_data)

    address_data = {
        "address_id": [1, 2, 3],
        "address_line_1": ["17", "23", "44"],
        "address_line_2": ["Green Street", "Yellow Road", None],
        "district": ["DistrictA", "DistrictB", "DistrictC"],
        "city": ["CityA", "CityB", "CityC"],
        "postal_code": ["123", "ABC", "987"],
        "country": ["UK", "USA", "Ireland"],
        "phone": [123, 456, 789],
        "created_at": [
            test_datetime,
            test_datetime,
            test_datetime],
        "last_updated": [
            test_datetime,
            test_datetime,
            test_datetime]
    }
    address_df = pd.DataFrame(data=address_data)

    test_dim_counterparty_data = {
        'counterparty_id': [1, 2, 3],
        'counterparty_legal_name': ['NameA', 'NameB', 'NameC'],
        'counterparty_legal_address_line_1': ["17", "23", "44"],
        'counterparty_legal_address_line_2': ["Green Street",
                                              "Yellow Road",
                                              None],
        'counterparty_legal_district': ["DistrictA",
                                        "DistrictB",
                                        "DistrictC"],
        'counterparty_legal_city': ["CityA",
                                    "CityB",
                                    "CityC"],
        'counterparty_legal_postal_code': ["123", "ABC", "987"],
        'counterparty_legal_country': ["UK", "USA", "Ireland"],
        'counterparty_legal_phone_number': [123, 456, 789]
    }
    dim_counterparty_df = pd.DataFrame(data=test_dim_counterparty_data)

    assert format_dim_counterparty(
        counterparty_df, address_df).equals(dim_counterparty_df)


def test_format_fact_sales_order_returns_expected_output():
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
    sales_order_df = pd.DataFrame(data=sales_order_data)
    test_fact_sales_order_data = {
        "sales_record_id": [1, 2, 3],
        "sales_order_id": [1, 2, 3],
        "created_date": [test_date, test_date, test_date],
        "created_time": [test_time, test_time, test_time],
        "last_updated_date": [test_date, test_date, test_date],
        "last_updated_time": [test_time, test_time, test_time],
        "staff_id": [1, 2, 3],
        "counterparty_id": [1, 2, 3],
        "units_sold": [100, 200, 300],
        "unit_price": [2.45, 3.67, 9.87],
        "currency_id": [1, 2, 3],
        "design_id": ["des1", "des2", "des3"],
        "agreed_delivery_date": [test_date, test_date, test_date],
        "agreed_payment_date": [test_date, test_date, test_date],
        "agreed_delivery_location_id": [1, 2, 3]
    }
    fact_sales_order_df = pd.DataFrame(data=test_fact_sales_order_data)
    assert format_fact_sales_order(sales_order_df).equals(fact_sales_order_df)


@patch("src.transformation.export_parquet_to_s3")
@patch("src.transformation.load_csv_from_s3")
def test_transform_data(mock_load, mock_export):

    mock_load.side_effect = load_func
    mock_export.return_value = True

    files_dict = {}
    files_dict["dim_staff.parquet"] = True
    files_dict["dim_location.parquet"] = True
    files_dict["dim_design.parquet"] = True
    files_dict["dim_date.parquet"] = True
    files_dict["dim_currency.parquet"] = True
    files_dict["dim_counterparty.parquet"] = True
    files_dict["fact_sales_order.parquet"] = True
    assert transform_data({}, {}) == files_dict


@patch("src.transformation.export_parquet_to_s3")
@patch("src.transformation.load_csv_from_s3")
def test_transform_data_load_errors(mock_load, mock_export, caplog):
    mock_load.side_effect = FileNotFoundError("Bucket not found.")
    mock_export.return_value = True
    transform_data({}, {})
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == "Error when retrieving files: "\
        "Bucket not found."


@patch("src.transformation.export_parquet_to_s3")
@patch("src.transformation.load_csv_from_s3")
def test_transform_data_formatting_errors(mock_load, mock_export, caplog):
    mock_load.side_effect = load_func_error
    mock_export.return_value = True
    transform_data({}, {})
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == "Error when formatting files: "\
        "Can only merge Series or DataFrame objects,"\
        " a <class 'bool'> was passed"


@patch("src.transformation.export_parquet_to_s3")
@patch("src.transformation.load_csv_from_s3")
def test_transform_data_exporting_errors(mock_load, mock_export, caplog):
    mock_load.side_effect = load_func
    mock_export.side_effect = AttributeError(
        "Object passed to the function is not of type DataFrame.")
    transform_data({}, {})
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].msg == "Error when uploading files: "\
        "Object passed to the function is not of type DataFrame."


def load_func(bucket, file, parse_dates=[]):
    if file == "staff.csv":
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
    elif file == "department.csv":
        dept_data = {
            "department_id": [1, 2, 3],
            "department_name": ["a", "b", "c"],
            "location": ["loca", "locb", "locc"],
            "manager": ["mrs a", "mr b", "ms c"],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=dept_data)
    elif file == "counterparty.csv":
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
    elif file == "currency.csv":
        currency_data = {
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR'],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=currency_data)
    elif file == "address.csv":
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
    elif file == "design.csv":
        design_data = {
            "design_id": [1, 2, 3],
            "created_at": [test_datetime, test_datetime, test_datetime],
            "design_name": ["design_c", "design_b", "design_a"],
            "file_location": ["loc_a", "loc_b", "loc/c"],
            "file_name": ["filename_a", "filename_b", "filename_c"],
            "last_updated": [test_datetime, test_datetime, test_datetime]
        }
        return pd.DataFrame(data=design_data)
    elif file == "sales_order.csv":
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


def load_func_error(bucket, file, parse_dates=[]):
    if file == "staff.csv":
        return True
    elif file == "departments.csv":
        return True
    elif file == "counterparty.csv":
        return True
    elif file == "currency.csv":
        return True
    elif file == "address.csv":
        return True
    elif file == "design.csv":
        return True
    elif file == "sales_order.csv":
        return True
