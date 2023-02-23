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
  sensitive   = true
}

# variable "warehouse_info" {
#   description = "placeholder for sensitive warehouse credentials"
#   sensitive   = true
# }
