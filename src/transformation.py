import boto3
import pandas as pd


def transform_data(event, context):
    """

    Args:
        event:
            either an S3 event or a Cloudwatch event - unsure as yet
        context:
            (we think) a valid AWS lambda Python context object - see
            https://docs.aws.amazon.com/lambda/latest/dg/python-context.html

    Raises:
        unsure as yet
        """

    s3 = boto3.client('s3')


# staff_data = pd.read_csv("test_files/test_data_staff.csv",
#                          parse_dates=["created_at", "last_updated"])

# dept_data = pd.read_csv("test_files/test_data_departments.csv",
#                         parse_dates=["created_at", "last_updated"])

# location_df = pd.read_csv(
#     "test_files/test_address.csv", parse_dates=["created_at", "last_updated"])

# design_df = pd.read_csv("test_files/test_design.csv",
#                         parse_dates=["created_at", "last_updated"])

# currency_df = pd.read_csv("test_files/test_currency.csv",
#                           parse_dates=["created_at", "last_updated"])

# with pd.option_context("display.max_columns", None):
#     print(dim_location)


def format_dim_staff(staff_df, dept_df):

    staff_dept = pd.merge(staff_df, dept_df,
                          how='left', on="department_id")
    dim_staff = staff_dept[["staff_id", "first_name", "last_name",
                            "department_name", "location", "email_address"]]
    return dim_staff


def format_dim_location(location_df):
    replacement_value = "None"
    location_df["address_line_2"].fillna(replacement_value, inplace=True)
    location_df["district"].fillna(replacement_value, inplace=True)
    location_df = location_df.rename(columns={"address_id": "location_id"})
    dim_location = location_df.iloc[:, 0:8]
    return dim_location


def format_dim_design(design_df):
    return design_df[["design_id",
                      "design_name", "file_location", "file_name"]]


def format_dim_date(start='2000-01-01', end='2049-12-31'):
    dim_date = pd.DataFrame({"date_id": pd.date_range(start, end)})
    dim_date["year"] = dim_date['date_id'].dt.year
    dim_date["month"] = dim_date['date_id'].dt.month
    dim_date["day"] = dim_date['date_id'].dt.day
    dim_date["day_of_week"] = dim_date['date_id'].dt.dayofweek
    dim_date["day_name"] = dim_date['date_id'].dt.day_name()
    dim_date["month_name"] = dim_date['date_id'].dt.month_name()
    dim_date["quarter"] = dim_date['date_id'].dt.quarter
    return dim_date


def format_dim_currency(currency_df):
    currency_df['currency_name'] = [
        'British Pound sterling', 'United States dollar', 'Euro']
    return currency_df[["currency_id", "currency_code", "currency_name"]]


def format_dim_counterparty(counterparty_df, address_df):
    location_df = format_dim_location(address_df)
    location_df = location_df.rename(
        columns={"location_id": "legal_address_id"})

    counterparty_df_full = pd.merge(counterparty_df, location_df,
                                    how='left', on="legal_address_id")

    dim_counterparty = counterparty_df_full[[
        'counterparty_id',
        'counterparty_legal_name',
        'address_line_1',
        'address_line_2',
        'district',
        'city',
        'postal_code',
        'country',
        'phone'
    ]]

    dim_counterparty = dim_counterparty.rename(columns={
        'address_line_1': 'counterparty_legal_address_line_1',
        'address_line_2': 'counterparty_legal_address_line_2',
        'district': 'counterparty_legal_district',
        'city': 'counterparty_legal_city',
        'postal_code': 'counterparty_legal_postal_code',
        'country': 'counterparty_legal_country',
        'phone': 'counterparty_legal_phone_number'
    })

    return dim_counterparty
