resource "aws_s3_bucket" "ingest-bucket" {
  bucket = "ingest-bucket-totedd-1402"
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket = "processed-bucket-totedd-1402"
}
