import logging
logger = logging.getLogger("processing")
logger.setLevel(logging.INFO)


def tbc(event, context):
    logger.info("starting_ingestion_lambda")
    print("Great Success!")
    logger.info("Finished_ingestion_lambda")

database_to_bucket_csv_file('staff', staff_columns, 'pandas-351803', 'staff.csv')
database_to_bucket_csv_file('transaction', transaction_columns, 'pandas-351803', 'transaction.csv')
database_to_bucket_csv_file('design', design_columns, 'pandas-351803', 'design.csv')
database_to_bucket_csv_file('address', address_columns, 'pandas-351803', 'address.csv')
database_to_bucket_csv_file('sales_order', sales_order_columns, 'pandas-351803', 'sales_orders.csv')
database_to_bucket_csv_file('counterparty', counterparty_columns, 'pandas-351803', 'counterparty.csv')
database_to_bucket_csv_file('payment', payment_columns, 'pandas-351803', 'payment.csv')
database_to_bucket_csv_file('payment_type', payment_type_columns, 'pandas-351803', 'payment_type.csv')
database_to_bucket_csv_file('currency', currency_columns, 'pandas-351803', 'currency.csv')
database_to_bucket_csv_file('department', department_columns, 'pandas-351803', 'department.csv')
database_to_bucket_csv_file('purchase_order', purchase_order_columns, 'pandas-351803', 'purchase_order.csv')