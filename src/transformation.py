""" Transforms data from a '.csv' file in AWS S3 to dataframes in 
Parquet format and uploads formatted data to S3.

The script consists of several formatting functions for each table
as well as functions to load and upload data to S3.

The script is intended to run on AWS Lambda.
"""

import boto3
import pandas as pd
import logging
from io import BytesIO
import os

logger = logging.getLogger("processing")
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')


def transform_data(event, context):
    """ Loads csv files from s3 ingestion bucket.
        Invokes formatting functions on the resulting dataframes.
        Converts dataframe to parquet format.
        Uploads to s3 processing bucket.

    Args:
        event: An AWS event object.
        context: A valid AWS lambda Python context object.

    Returns:
        A dictionary with keys of the files to be uploaded
        values True if file successfully processed, else None.
    """
    INGEST_BUCKET = os.environ.get('TF_ING_BUCKET')
    PROCESSED_BUCKET = os.environ.get('TF_PRO_BUCKET')

    files_list = ["staff.csv", "department.csv", "address.csv",
                  "design.csv", "counterparty.csv", "transaction.csv",
                  "payment_type.csv", "currency.csv"]
    try:
        df_list = [load_csv_from_s3(
            INGEST_BUCKET, file, parse_dates=["created_at", "last_updated"])
            for file in files_list]
        sales_order_df = load_csv_from_s3(INGEST_BUCKET, "sales_order.csv",
                                          parse_dates=["created_at",
                                                       "last_updated",
                                                       "agreed_delivery_date",
                                                       "agreed_payment_date"])
        purchase_order_df = load_csv_from_s3(
            INGEST_BUCKET, "purchase_order.csv",
            parse_dates=["created_at", "last_updated",
                         "agreed_delivery_date", "agreed_payment_date"])
        payment_df = load_csv_from_s3(
            INGEST_BUCKET, "payment.csv", parse_dates=["created_at",
                                                       "last_updated",
                                                       "payment_date"])
        (staff_df, dept_df, address_df, design_df, counterparty_df,
         transaction_df, payment_type_df, currency_df) = df_list
    except Exception as error:
        logger.error(f'Error when retrieving files: {error}')

    try:
        files_dict = {}
        files_dict["dim_staff.parquet"] = format_dim_staff(staff_df, dept_df)
        files_dict["dim_location.parquet"] = format_dim_location(address_df)
        files_dict["dim_design.parquet"] = format_dim_design(design_df)
        files_dict["dim_date.parquet"] = format_dim_date(
            start='2020-01-01', end='2024-12-31')
        files_dict["dim_currency.parquet"] = format_dim_currency(currency_df)
        files_dict["dim_counterparty.parquet"] = format_dim_counterparty(
            counterparty_df, address_df)
        files_dict["dim_transaction.parquet"] = format_dim_transaction(
            transaction_df)
        files_dict["dim_payment_type.parquet"] = format_dim_payment_type(
            payment_type_df)
        files_dict["fact_sales_order.parquet"] = format_fact_sales_order(
            sales_order_df)
        files_dict["fact_purchase_order.parquet"] = format_fact_purchase_order(
            purchase_order_df)
        files_dict["fact_payment.parquet"] = format_fact_payment(
            payment_df)
    except Exception as error:
        logger.error(f'Error when formatting files: {error}')

    try:
        upload_results = {item: export_parquet_to_s3(
            files_dict[item], PROCESSED_BUCKET, item) for item in files_dict}
        logger.info('SUCCESSFULLY PROCESSED')
        return upload_results
    except Exception as error:
        logger.error(f'Error when uploading files: {error}')


def format_dim_staff(staff_df, dept_df):
    """ Formats staff and department dataframes into
        correctly formatted dataframe.

    Args:
        staff_df: Dataframe containing data of staff.csv.
        dept_df: Dataframe containing data of departments.csv.

    Returns:
        Dataframe of correctly formatted staff data.
    """
    staff_dept = pd.merge(staff_df, dept_df,
                          how='left', on="department_id")
    dim_staff = staff_dept[["staff_id", "first_name", "last_name",
                            "department_name", "location", "email_address"]]
    return dim_staff


def format_dim_location(location_df):
    """ Formats location dataframe into correctly formatted dataframe.

    Args:
        location_df: Dataframe containing data of addresses.csv.

    Returns:
        Dataframe of correctly formatted location data.
    """
    location_df = location_df.rename(columns={"address_id": "location_id"})
    dim_location = location_df.iloc[:, 0:8]
    return dim_location


def format_dim_design(design_df):
    """ Formats design dataframe into correctly formatted dataframe.

    Args:
        design_df: Dataframe containing data of design.csv.

    Returns:
        Dataframe of correctly formatted design data.
    """
    return design_df[["design_id",
                      "design_name", "file_location", "file_name"]]


def format_dim_date(start='2000-01-01', end='2049-12-31'):
    """ Formats given dates into correctly formatted dataframe.

    Args:
        start: The first date to be included in the dataframe.
               Defaults to 2000-01-01.
        end: The end date to be included in the dataframe.
             Defaults to 2049-12-31.

    Returns:
            Dataframe of correctly formatted date data.
    """
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
    """ Formats currency dataframe into correctly formatted dataframe.

    Args:
        currency_df: Dataframe containing data from currency.csv.

    Returns:
        Dataframe of correctly formatted currency data.

    """
    currency_df['currency_name'] = [
        'British Pound sterling', 'United States dollar', 'Euro']
    return currency_df[["currency_id", "currency_code", "currency_name"]]


def format_dim_counterparty(counterparty_df, address_df):
    """ Formats counterparty and address dataframes
        into a correctly formatted dataframe.

    Args:
        counterparty_df: Dataframe containing data from counterparty.csv.
        address_df: Dataframe containing data from address.csv.

    Returns:
        Dataframe of correctly formatted counterparty data.
    """
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


def format_dim_transaction(transaction_df):
    """ Formats transaction dataframe into correctly formatted dataframe.

    Args:
        transaction_df: Dataframe containing data from transaction.csv.

    Returns:
        Dataframe of correctly formatted transaction data.
    """
    transaction_df = transaction_df[["transaction_id", "transaction_type",
                                     "sales_order_id", "purchase_order_id"]]
    transaction_df = transaction_df.astype({'sales_order_id': 'Int64',
                                            'purchase_order_id': 'Int64'})
    return transaction_df


def format_dim_payment_type(payment_df):
    """ Formats payment dataframe into correctly formatted dataframe.

    Args:
        payment_df: Dataframe containing data from payment.csv.

    Returns:
        Dataframe of correctly formatted payment data.
    """
    return payment_df[["payment_type_id", "payment_type_name"]]


def format_fact_sales_order(sales_order_df):
    """ Formats sales_order dataframe into correctly formatted
    dataframe.

    Args:
        sales_order_df: Dataframe of data from sales_order.csv.

    Returns:
        Dataframe of correctly formatted sales order data.
    """
    sales_order_df["sales_record_id"] = range(1, 1+len(sales_order_df))
    sales_order_df["created_date"] = sales_order_df["created_at"].dt.date
    sales_order_df["created_time"] = sales_order_df["created_at"].dt.time
    sales_order_df["last_updated_date"] = sales_order_df[
        "last_updated"].dt.date
    sales_order_df["last_updated_time"] = sales_order_df[
        "last_updated"].dt.time
    sales_order_df["sales_staff_id"] = sales_order_df["staff_id"]

    fact_sales_order = sales_order_df[[
        "sales_record_id",
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id"
    ]]
    return fact_sales_order


def format_fact_purchase_order(purchase_order_df):
    """ Formats purchase_order dataframe into correctly formatted
    dataframe.

    Args:
        purchase_order_df: Dataframe of data from purchase_order.csv.

    Returns:
        Dataframe of correctly formatted purchase order data.
    """
    purchase_order_df["purchase_record_id"] = range(
        1, 1+len(purchase_order_df))
    purchase_order_df["created_date"] = purchase_order_df["created_at"].dt.date
    purchase_order_df["created_time"] = purchase_order_df["created_at"].dt.time
    purchase_order_df["last_updated_date"] = purchase_order_df[
        "last_updated"].dt.date
    purchase_order_df["last_updated_time"] = purchase_order_df[
        "last_updated"].dt.time

    fact_purchase_order = purchase_order_df[[
        "purchase_record_id",
        "purchase_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "staff_id",
        "counterparty_id",
        "item_code",
        "item_quantity",
        "item_unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id"
    ]]
    return fact_purchase_order


def format_fact_payment(payment_df):
    """ Formats payment dataframe into correctly formatted dataframe.

    Args:
        payment_df: Dataframe of data from payment.csv.

    Returns:
        Dataframe of correctly formatted payment data.
    """
    payment_df["payment_record_id"] = range(1, 1+len(payment_df))
    payment_df["created_date"] = payment_df["created_at"].dt.date
    payment_df["created_time"] = payment_df["created_at"].dt.time
    payment_df["last_updated_date"] = payment_df[
        "last_updated"].dt.date
    payment_df["last_updated_time"] = payment_df[
        "last_updated"].dt.time

    fact_payment = payment_df[[
        "payment_record_id",
        "payment_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "transaction_id",
        "counterparty_id",
        "payment_amount",
        "currency_id",
        "payment_type_id",
        "paid",
        "payment_date"
    ]]
    return fact_payment


def load_csv_from_s3(bucket, key, parse_dates=[]):
    """ Retrieve a CSV file from an S3 bucket.

    Args:
        bucket: Name of the S3 bucket from which to retrieve the file.
        key: Key that the file is stored under in the named S3 bucket.
        parse_dates: A list of column names which contain dates to be
        converted to datetime objects.

    Returns:
        DataFrame containing the contents of the CSV file.

    Raises:
        NoSuchBucket
        NoSuchKey
        RuntimeError
    """
    try:
        s3_response_object = s3.get_object(
            Bucket=bucket, Key=key)
        df = s3_response_object['Body'].read()
        df = pd.read_csv(BytesIO(df), parse_dates=parse_dates)
        return df
    except s3.exceptions.NoSuchBucket:
        logger.error('Bucket does not exist')
        raise s3.exceptions.NoSuchBucket({}, '')
    except s3.exceptions.NoSuchKey:
        logger.error("Key not found in bucket")
        raise s3.exceptions.NoSuchKey({}, '')
    except Exception as e:
        logger.error(e)
        raise RuntimeError


def export_parquet_to_s3(data, bucket, key):
    """ Convert DataFrame to parquet file and store in an S3 bucket

    Args:
        data: Dataframe containing the data to store in the parquet
        file.
        bucket: Name of the S3 bucket in which to store the file.
        key: Key that the file will be stored under in the named
        S3 bucket.

    Returns:
        True if the function execution was successful, else None.

    Raises:
        FileNotFoundError
        ValueError
        AttributeError
        RuntimeError
    """
    try:
        data.to_parquet(f's3://{bucket}/{key}', index=True)
        return True
    except FileNotFoundError:
        logger.error("Bucket not found.")
        raise FileNotFoundError
    except ValueError:
        logger.error("Key not of correct format.")
        raise ValueError
    except AttributeError:
        logger.error(
            "Object passed to the function is not of type DataFrame.")
        raise AttributeError
    except Exception as e:
        logger.error(e)
        raise RuntimeError
