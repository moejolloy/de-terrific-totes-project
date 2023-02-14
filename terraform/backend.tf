terraform {
  backend "s3" {
    bucket  = "terraform-state-bucket-totedd-1402"
    key     = "terraform/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}
