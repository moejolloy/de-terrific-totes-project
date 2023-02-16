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
    location_df = location_df.rename(columns={"address_id": "location_id"})
    dim_location = location_df.iloc[:, 0:8]
    return dim_location


def format_dim_design(design_df):
    return design_df[["design_id",
                      "design_name", "file_location", "file_name"]]
