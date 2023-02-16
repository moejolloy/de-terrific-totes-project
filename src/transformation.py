import boto3


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
