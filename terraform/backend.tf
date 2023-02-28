terraform {
  backend "s3" {
    bucket  = "terrific-totes-terraform-bucket-revised"
    key     = "terraform/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
  required_version = "~> 1.3.7"
}
