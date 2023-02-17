import boto3
import pandas as pd
from io import BytesIO
import logging

logger = logging.getLogger("processing")
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


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
    staff_df = load_csv_from_s3("nc-marie-c-demo", "staff.csv")
    dept_df = load_csv_from_s3("nc-marie-c-demo", "departments.csv")
    counterparty_df = load_csv_from_s3("nc-marie-c-demo", "counterparty.csv")
    currency_df = load_csv_from_s3("nc-marie-c-demo", "currency.csv")
    design_df = load_csv_from_s3("nc-marie-c-demo", "design.csv")
    sales_order_df = load_csv_from_s3("nc-marie-c-demo", "sales_order.csv")
    address_df = load_csv_from_s3("nc-marie-c-demo", "address.csv")

    dim_staff = format_dim_staff(staff_df, dept_df)
    dim_location = format_dim_location(address_df)
    dim_design = format_dim_design(design_df)
    dim_date = format_dim_date(start='2020-01-01', end='2024-12-31')
    dim_currency = format_dim_currency(currency_df)
    dim_counterparty = format_dim_counterparty(counterparty_df, address_df)
    fact_sales_order = format_fact_sales_order(sales_order_df)

    export_parquet_to_s3(
        dim_staff, "nc-marie-c-processed", "dim_staff.parquet")
    export_parquet_to_s3(
        dim_location, "nc-marie-c-processed", "dim_location.parquet")
    export_parquet_to_s3(
        dim_design, "nc-marie-c-processed", "dim_design.parquet")
    export_parquet_to_s3(dim_date, "nc-marie-c-processed",
                         "dim_design.parquet")
    export_parquet_to_s3(
        dim_currency, "nc-marie-c-processed", "dim_currency.parquet")
    export_parquet_to_s3(
        dim_counterparty, "nc-marie-c-processed", "dim_counterparty.parquet")
    export_parquet_to_s3(
        fact_sales_order, "nc-marie-c-processed", "fact_sales_order.parquet")


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


def format_fact_sales_order(sales_order_df):

    sales_order_df["sales_record_id"] = range(1, 1+len(sales_order_df))
    sales_order_df["created_date"] = sales_order_df["created_at"].dt.date
    sales_order_df["created_time"] = sales_order_df["created_at"].dt.time
    sales_order_df["last_updated_date"] = sales_order_df["last_updated"].dt.date
    sales_order_df["last_updated_time"] = sales_order_df["last_updated"].dt.time

    fact_sales_order = sales_order_df[[
        "sales_record_id",
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id"
    ]]
    print(fact_sales_order)
    return fact_sales_order


def format_fact_purchase_order(purchase_order_df):
    pass


# utils functions

def load_csv_from_s3(bucket, key):
    """ Retrieve a CSV file from an S3 bucket

    Args:
        bucket: Name of the S3 bucket from which to retrieve the file.
        key: Key that the file is stored under in the named S3 bucket.

    Returns:
        DataFrame containing the contents of the CSV file.
    """
    try:
        s3_response_object = s3.get_object(
            Bucket=bucket, Key=key)
        df = s3_response_object['Body'].read()
        df = pd.read_csv(BytesIO(df))
        return df
    except s3.exceptions.NoSuchBucket:
        logger.critical('Bucket does not exist')
        raise s3.exceptions.NoSuchBucket({}, '')
    except s3.exceptions.NoSuchKey:
        logger.critical("Key not found in bucket")
        raise s3.exceptions.NoSuchKey({}, '')
    except Exception as e:
        logger.critical(e)
        raise RuntimeError


def export_parquet_to_s3(data, bucket, key):
    """ Convert DataFrame to parquet file and store in an S3 bucket

    Args:
        data: Dataframe containing the data to store in the parquet file.
        bucket: Name of the S3 bucket in which to store the file.
        key: Key that the file will be stored under in the named S3 bucket.

    Returns:
        True if the function execution was successful, else None.
    """
    try:
        data.to_parquet(f's3://{bucket}/{key}', index=True)
        return True
    except FileNotFoundError:
        logger.critical("Bucket not found.")
        raise FileNotFoundError
    except ValueError:
        logger.critical("Key not of correct format.")
        raise ValueError
    except AttributeError:
        logger.critical(
            "Object passed to the function is not of type DataFrame.")
        raise AttributeError
    except Exception as e:
        logger.critical(e)
        raise RuntimeError
