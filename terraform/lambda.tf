resource "aws_lambda_function" "dummy-file-reader" {
  filename      = "${path.module}/../zips/reader.zip"
  function_name = var.dummy_lambda_name
  role          = aws_iam_role.ingest-lambda-role.arn
  handler       = "reader.lambda_handler"
  runtime       = "python3.9"
}

resource "aws_lambda_permission" "allow_s3" {
  action         = "lambda:InvokeFunction"
  function_name  = aws_lambda_function.dummy-file-reader.function_name
  principal      = "s3.amazonaws.com"
  source_arn     = aws_s3_bucket.ingest-bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}
