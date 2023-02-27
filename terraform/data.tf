data "aws_caller_identity" "current" {}

data "aws_region" "current" {}


data "archive_file" "ingestion_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/zips/ingestion-files"
  output_path = "${path.module}/zips/ingestion.zip"
  depends_on  = [null_resource.pip_install_ingestion_dependencies, null_resource.copy_ingestion_file_for_zipping]
}

data "archive_file" "processing_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/zips/processing-files"
  output_path = "${path.module}/zips/transformation.zip"
  depends_on = [null_resource.pip_install_processing_dependencies,
  null_resource.copy_processing_file_for_zipping]
}

data "archive_file" "population_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/zips/population-files"
  output_path = "${path.module}/zips/population.zip"
  depends_on = [null_resource.pip_install_population_dependencies,
  null_resource.copy_population_file_for_zipping]
}

resource "null_resource" "pip_install_ingestion_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/lambda-dependencies/ingestion-dependencies.txt"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "pip install -r lambda-dependencies/ingestion-dependencies.txt -t ${path.module}/zips/ingestion-files"
  }
}

resource "null_resource" "copy_ingestion_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/ingestion.py"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp ../src/ingestion.py ${path.module}/zips/ingestion-files"
  }
}

resource "null_resource" "pip_install_processing_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/lambda-dependencies/processing-dependencies.txt"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "pip install -r lambda-dependencies/processing-dependencies.txt -t ${path.module}/zips/processing-files"
  }
}

resource "null_resource" "copy_processing_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/transformation.py"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp ../src/transformation.py ${path.module}/zips/processing-files"
  }
}

resource "null_resource" "pip_install_population_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/lambda-dependencies/population-dependencies.txt"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "pip install -r lambda-dependencies/population-dependencies.txt -t ${path.module}/zips/population-files"
  }
}

resource "null_resource" "copy_population_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/population.py"))}"
    always_run = "${timestamp()}"
  }

  provisioner "local-exec" {
    command = "cp ../src/population.py ${path.module}/zips/population-files"
  }
}