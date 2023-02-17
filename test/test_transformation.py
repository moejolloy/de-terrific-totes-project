from unittest.mock import Mock, patch
import pandas as pd
from src.transformation import format_dim_staff, format_dim_location, format_dim_design, format_dim_date, format_dim_currency, format_dim_counterparty, format_fact_sales_order
import datetime


def test_format_dim_staff_returns_expected_output():
    staff_data = {"staff_id": [1, 2, 3], "first_name": [
        "Sam", "Joe", "Max"], "last_name": ["Ant", "Boo", "Car"], "department_id": [1, 2, 3], "email_address": ["a@b.com", "b@c.com", "c@d.com"], "created_at": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")], "last_updated": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]}
    staff_df = pd.DataFrame(data=staff_data)
    dept_data = {"department_id": [1, 2, 3], "department_name": ["a", "b", "c"], "location": ["loca", "locb", "locc"], "manager": ["mrs a", "mr b", "ms c"], "created_at": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")], "last_updated": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]
                 }
    dept_df = pd.DataFrame(data=dept_data)

    test_dim_staff_data = {"staff_id": [1, 2, 3], "first_name": [
        "Sam", "Joe", "Max"], "last_name": ["Ant", "Boo", "Car"], "department_name": ["a", "b", "c"], "location": ["loca", "locb", "locc"], "email_address": ["a@b.com", "b@c.com", "c@d.com"]
    }
    dim_staff_df = pd.DataFrame(data=test_dim_staff_data)
    assert format_dim_staff(staff_df, dept_df).equals(dim_staff_df)


def test_format_dim_location_returns_expected_output():
    location_data = {"address_id": [1, 2, 3], "address_line_1": [
        "17", "23", "44"], "address_line_2": ["Green Street", "Yellow Road", None], "district": ["districta", "districtb", "districtc"], "city": ["citya", "cityb", "cityc"], "postal_code": ["123", "ABC", "987"], "country": ["Uk", "USA", "Ireland"], "phone": [123, 456, 789], "created_at": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")], "last_updated": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]}
    location_df = pd.DataFrame(data=location_data)

    test_dim_location_data = {
        "location_id": [1, 2, 3], "address_line_1": [
            "17", "23", "44"], "address_line_2": ["Green Street", "Yellow Road", "None"], "district": ["districta", "districtb", "districtc"], "city": ["citya", "cityb", "cityc"], "postal_code": ["123", "ABC", "987"], "country": ["Uk", "USA", "Ireland"], "phone": [123, 456, 789]
    }
    dim_location_df = pd.DataFrame(data=test_dim_location_data)
    assert format_dim_location(location_df).equals(dim_location_df)


def test_format_dim_design_returns_expected_output():
    design_data = {"design_id": [1, 2, 3], "created_at": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")], "design_name": [
        "design_c", "design_b", "design_a"], "file_location": ["loc_a", "loc_b", "loc/c"], "file_name": ["filename_a", "filename_b", "filename_c"], "last_updated": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]}
    design_df = pd.DataFrame(data=design_data)

    test_dim_design_data = {"design_id": [1, 2, 3], "design_name": [
        "design_c", "design_b", "design_a"], "file_location": ["loc_a", "loc_b", "loc/c"], "file_name": ["filename_a", "filename_b", "filename_c"]
    }
    dim_design_df = pd.DataFrame(data=test_dim_design_data)
    assert format_dim_design(design_df).equals(dim_design_df)


def test_format_dim_date_returns_expected_output():
    start = '2000-01-01'
    end = '2000-01-10'

    test_dim_date_data = {"date_id": [datetime.datetime.fromisoformat('2000-01-01'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-02'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-03'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-04'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-05'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-06'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-07'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-08'),
                                      datetime.datetime.fromisoformat(
                                          '2000-01-09'),
                                      datetime.datetime.fromisoformat('2000-01-10')],
                          "year": [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000],
                          "month": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                          "day": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                          "day_of_week": [5, 6, 0, 1, 2, 3, 4, 5, 6, 0],
                          "day_name": ['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday', 'Monday'],
                          "month_name": ['January', 'January', 'January', 'January', 'January', 'January', 'January', 'January', 'January', 'January'],
                          "quarter": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]}

    dim_date_df = pd.DataFrame(data=test_dim_date_data)
    assert format_dim_date(start, end).equals(dim_date_df)


def test_format_dim_currency_returns_expected_output():
    currency_data = {'currency_id': [1, 2, 3], 'currency_code': ['GBP', 'USD', 'EUR'],
                     "created_at": [
        datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
        datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
        datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")],
        "last_updated": [
        datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
        datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
        datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]}
    currency_df = pd.DataFrame(data=currency_data)

    test_dim_currency_data = {'currency_id': [1, 2, 3], 'currency_code': [
        'GBP', 'USD', 'EUR'], 'currency_name': ['British Pound sterling', 'United States dollar', 'Euro']}
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
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")],
        'last_updated': [
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]
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
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")],
        "last_updated": [
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"),
            datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")]}
    address_df = pd.DataFrame(data=address_data)

    test_dim_counterparty_data = {
        'counterparty_id': [1, 2, 3],
        'counterparty_legal_name': ['NameA', 'NameB', 'NameC'],
        'counterparty_legal_address_line_1': ["17", "23", "44"],
        'counterparty_legal_address_line_2': ["Green Street", "Yellow Road", 'None'],
        'counterparty_legal_district': ["DistrictA", "DistrictB", "DistrictC"],
        'counterparty_legal_city': ["CityA", "CityB", "CityC"],
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
        "created_at": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")],
        "last_updated": [datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000"), datetime.datetime.fromisoformat("2022-11-03T14:20:51.563000")],
        "design_id": ["des1", "des2", "des3"],
        "staff_id": [1, 2, 3],
        "counterparty_id": [1, 2, 3],
        "units_sold": [100, 200, 300],
        "unit_price": [2.45, 3.67, 9.87],
        "currency_id": [1, 2, 3],
        "agreed_delivery_date": [datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03')],
        "agreed_payment_date": [datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03')],
        "agreed_delivery_location_id": [1, 2, 3]
    }
    sales_order_df = pd.DataFrame(data=sales_order_data)
    test_fact_sales_order_data = {
        "sales_record_id": [1, 2, 3],
        "sales_order_id": [1, 2, 3],
        "created_date": [datetime.date.fromisoformat("2022-11-03"), datetime.date.fromisoformat("2022-11-03"), datetime.date.fromisoformat("2022-11-03")],
        "created_time": [datetime.time.fromisoformat("14:20:51.563000"), datetime.time.fromisoformat("14:20:51.563000"), datetime.time.fromisoformat("14:20:51.563000")],
        "last_updated_date": [datetime.date.fromisoformat("2022-11-03"), datetime.date.fromisoformat("2022-11-03"), datetime.date.fromisoformat("2022-11-03")],
        "last_updated_time": [datetime.time.fromisoformat("14:20:51.563000"), datetime.time.fromisoformat("14:20:51.563000"), datetime.time.fromisoformat("14:20:51.563000")],
        "staff_id": [1, 2, 3],
        "counterparty_id": [1, 2, 3],
        "units_sold": [100, 200, 300],
        "unit_price": [2.45, 3.67, 9.87],
        "currency_id": [1, 2, 3],
        "design_id": ["des1", "des2", "des3"],
        "agreed_delivery_date": [datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03')],
        "agreed_payment_date": [datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03'), datetime.datetime.fromisoformat('2022-11-03')],
        "agreed_delivery_location_id": [1, 2, 3]
    }
    fact_sales_order_df = pd.DataFrame(data=test_fact_sales_order_data)
    assert format_fact_sales_order(sales_order_df).equals(fact_sales_order_df)
