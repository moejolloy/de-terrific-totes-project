data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


# Archive_file will archive a file and zip it when running terraform apply
# we can do this for all python files

data "archive_file" "dummy_lambda_zipper" {
  type        = "zip"
  source_file = "${path.module}/../src/reader.py"
  output_path = "${path.module}/../zips/reader.zip"
}

data "archive_file" "ingestion_lambda_zipper" {
  type        = "zip"
  source_file = "${path.module}/../src/ingestion.py"
  output_path = "${path.module}/../zips/ingestion.zip"
}

data "archive_file" "processing_lambda_zipper" {
  type        = "zip"
  source_file = "${path.module}/../src/transformation.py"
  output_path = "${path.module}/../zips/transformation.zip"
}

data "archive_file" "population_lambda_zipper" {
  type        = "zip"
  source_file = "${path.module}/../src/population.py"
  output_path = "${path.module}/../zips/population.zip"
}
