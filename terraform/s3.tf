resource "aws_s3_bucket" "ingest-bucket" {
  bucket_prefix = "ingest-bucket-totedd-1402"
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket_prefix = "processed-bucket-totedd-1403"
}

resource "aws_s3_bucket_notification" "dummy_lambda_bucket_notification" {
  bucket = aws_s3_bucket.ingest-bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.dummy-file-reader.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}
