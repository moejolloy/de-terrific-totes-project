resource "aws_s3_bucket" "ingest-bucket" {
  bucket_prefix = "terrific-totes-ingest-bucket-"
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket_prefix = "terrific-totes-processed-bucket-"
}
