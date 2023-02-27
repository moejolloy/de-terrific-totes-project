data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


data "archive_file" "ingestion_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/../zips/ingestion_files"
  output_path = "${path.module}/../zips/ingestion.zip"
  depends_on  = [null_resource.pip_install_ingestion_dependencies, null_resource.copy_ingestion_file_for_zipping]
}

data "archive_file" "processing_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/../zips/processing_files"
  output_path = "${path.module}/../zips/transformation.zip"
  depends_on = [null_resource.pip_install_processing_dependencies,
  null_resource.copy_processing_file_for_zipping]
}

data "archive_file" "population_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/../zips/population_files"
  output_path = "${path.module}/../zips/population.zip"
  depends_on = [null_resource.pip_install_population_dependencies,
  null_resource.copy_population_file_for_zipping]
}

resource "null_resource" "pip_install_ingestion_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/ingestion_dependencies.txt"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "pip install -r ingestion_dependencies.txt -t ${path.module}/../zips/ingestion_files"
  }
}

resource "null_resource" "copy_ingestion_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/ingestion.py"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp ../src/ingestion.py ${path.module}/../zips/ingestion_files"
  }
}

resource "null_resource" "pip_install_processing_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/processing_dependencies.txt"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "pip install -r processing_dependencies.txt -t ${path.module}/../zips/processing_files"
  }
}

resource "null_resource" "copy_processing_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/transformation.py"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp ../src/transformation.py ${path.module}/../zips/processing_files"
  }
}

resource "null_resource" "pip_install_population_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/population_dependencies.txt"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "pip install -r population_dependencies.txt -t ${path.module}/../zips/population_files"
  }
}

resource "null_resource" "copy_population_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/population.py"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp ../src/population.py ${path.module}/../zips/population_files"
  }
}