resource "aws_s3_bucket" "ingest-bucket" {
  bucket = "terrific-totes-ingest-bucket-25"
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket = "terrific-totes-processed-bucket-25"
}


