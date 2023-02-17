# used file-reader as an example - see below:
# variable "lambda_name" {
#     type = string
#     default = "s3-file-reader"
# }

variable "dummy_lambda_name" {
  type    = string
  default = "dummy-file-reader"
}

variable "ingestion_lambda_name" {
  type    = string
  default = "ingestion-lambda"
}

variable "processing_lambda_name" {
  type    = string
  default = "processing-lambda"
}

variable "population_lambda_name" {
  type    = string
  default = "population-lambda"
}

variable "database_info" {
  description = "placeholder for sensitive database credentials"
  type = string
  sensitive = true
}