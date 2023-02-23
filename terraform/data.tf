data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

data "archive_file" "ingestion_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../zips/ingestion.zip"
  depends_on  = [data.archive_file.ingestion_dependencies]
}

data "archive_file" "processing_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../zips/processing.zip"
  depends_on  = [data.archive_file.processing_dependencies]
}

data "archive_file" "population_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../zips/population.zip"
  depends_on  = [data.archive_file.population_dependencies]
}

data "archive_file" "ingestion_dependencies" {
  type        = "zip"
  source_dir  = "${path.module}/../"
  output_path = "${path.module}/../zips/ingestion_dependencies.zip"
}

data "archive_file" "processing_dependencies" {
  type        = "zip"
  source_dir  = "${path.module}/../"
  output_path = "${path.module}/../zips/processing_dependencies.zip"
}

data "archive_file" "population_dependencies" {
  type        = "zip"
  source_dir  = "${path.module}/../"
  output_path = "${path.module}/../zips/population_dependencies.zip"
}

resource "aws_lambda_function" "ingestion" {
  filename      = "${data.archive_file.ingestion_zip.output_path}"
  function_name = "ingestion_lambda"
  role          = aws_iam_role.ingestion_lambda.arn
  handler       = "ingestion.lambda_handler"
  runtime       = "python3.8"
}

resource "aws_lambda_function" "processing" {
  filename      = "${data.archive_file.processing_zip.output_path}"
  function_name = "processing_lambda"
  role          = aws_iam_role.processing_lambda.arn
  handler       = "processing.lambda_handler"
  runtime       = "python3.8"
}

resource "aws_lambda_function" "population" {
  filename      = "${data.archive_file.population_zip.output_path}"
  function_name = "population_lambda"
  role          = aws_iam_role.population_lambda.arn
  handler       = "population.lambda_handler"
  runtime       = "python3.8"
}