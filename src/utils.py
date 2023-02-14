import boto3
import pandas as pd


"""
Function accepts a CSV file
Reformats file and outputs as parquet
"""


def csv_to_parquet_transformer(csv):
    data = pd.read_csv(csv)
    data.to_parquet("test_files/Results.parquet")
    x = pd.read_parquet("test_files/Results.parquet")
    print(x)


csv_to_parquet_transformer('test_files/Results.csv')
