resource "aws_s3_bucket" "ingest-bucket" {
  bucket = "terrific-totes-ingest-bucket-21"
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket = "terrific-totes-processed-bucket-21"
}


