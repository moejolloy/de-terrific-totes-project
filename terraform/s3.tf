resource "aws_s3_bucket" "ingest-bucket" {
  bucket = "terrific-totes-ingest-bucket-1"
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket = "terrific-totes-processed-bucket-1"
}

resource "aws_s3_bucket_notification" "dummy_lambda_bucket_notification" {
  bucket = aws_s3_bucket.ingest-bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.dummy-file-reader.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_dummy_file_reader]
}
