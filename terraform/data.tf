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

resource "null_resource" "pip_install_layer_1" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../layer1.txt"))}"
  }

  provisioner "local-exec" {
    command = "pip install -r ../layer1.txt -t ${path.module}/../zips/layer1"
  }
}

resource "null_resource" "pip_install_layer_2" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../layer2.txt"))}"
  }

  provisioner "local-exec" {
    command = "pip install -r ../layer2.txt -t ${path.module}/../zips/layer2"
  }
}

data "archive_file" "layer_1_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/../zips/layer1"
  output_path = "${path.module}/../zips/layer_1.zip"
  depends_on  = [null_resource.pip_install_layer_1]
}

data "archive_file" "layer_2_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/../zips/layer2"
  output_path = "${path.module}/../zips/layer_2.zip"
  depends_on  = [null_resource.pip_install_layer_2]
}
