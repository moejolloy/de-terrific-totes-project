from unittest.mock import Mock, patch
import pandas as pd
from src.transformation import format_dim_staff, format_dim_location, format_dim_design
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
