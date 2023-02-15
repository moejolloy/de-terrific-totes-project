data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


# Archive_file will archive a file and zip it when running terraform apply
# we can do this for all python files

data "archive_file" "file_reader" {
  type        = "zip"
  source_file = "${path.module}/../src/reader.py"
  output_path = "${path.module}/../zips/reader.zip"
}
