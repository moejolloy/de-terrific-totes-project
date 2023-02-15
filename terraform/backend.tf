terraform {
  backend "s3" {
    bucket  = "terraform-state-bucket-totedd-1503"
    key     = "terraform/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
  required_version = "~> 1.3.7"
}

