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
  source_dir  = "${path.module}/../zips/ingestion_files"
  output_path = "${path.module}/../zips/ingestion.zip"
  depends_on  = [null_resource.pip_install_ingestion_dependencies, null_resource.copy_ingestion_file_for_zipping]
}

data "archive_file" "processing_lambda_zipper" {
  type        = "zip"
  source_dir  = "${path.module}/../zips/processing_files"
  output_path = "${path.module}/../zips/transformation.zip"
  depends_on  = [null_resource.pip_install_processing_dependencies, null_resource.copy_processing_file_for_zipping]
}

data "archive_file" "population_lambda_zipper" {
  type        = "zip"
  source_file = "${path.module}/../src/population.py"
  output_path = "${path.module}/../zips/population.zip"
}

resource "null_resource" "pip_install_ingestion_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../ingestion_dependencies.txt"))}"
  }

  provisioner "local-exec" {
    command = "pip install -r ../ingestion_dependencies.txt -t ${path.module}/../zips/ingestion_files"
  }
}

resource "null_resource" "copy_ingestion_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/ingestion.py"))}"
  }

  provisioner "local-exec" {
    command = "cp ../src/ingestion.py ${path.module}/../zips/ingestion_files"
  }
}

resource "null_resource" "pip_install_processing_dependencies" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../processing_dependencies.txt"))}"
  }

  provisioner "local-exec" {
    command = "pip install -r ../processing_dependencies.txt -t ${path.module}/../zips/processing_files"
  }
}

resource "null_resource" "copy_processing_file_for_zipping" {
  triggers = {
    shell_hash = "${sha256(file("${path.module}/../src/transformation.py"))}"
  }

  provisioner "local-exec" {
    command = "cp ../src/transformation.py ${path.module}/../zips/processing_files"
  }
}
