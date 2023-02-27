# de-terrific-totes-project

**Project Summary**

- corresponds to Data section of Joe's [project specification](https://github.com/northcoders/de-project-specification) repo.
- keep Terrific Totes/business requirements at forefront of your mind when writing this section. i.e Terrific totes want the data to be in this format!

**Directions for Deployment**

Split deploying the infrastructure into two sections - locally and using Github Actions with a fork of the repo
1. Using Github actions:
	- save database credentials, warehouse credentials and AWS access and secret keys as 4 separate GitHub actions secrets.
	- Take note of correct formatting of data warehouse and database credentials - make an example of this in file.
	- Either select manual workflow deployment on github actions page, or push some code to main branch
2. Locally:
	- run 'make all' command in root directory
	-  aws configure using aws login details
	-  run aws sts get-caller-identity to check you are logged in correctly
	- run the terraform state bucket creation command
    ```sh
    aws s3 mb s3://terraform-state-bucket-totedd-<INSERT-BUCKET-NAME-HERE>
    ```
	- run terraform init - reconfigure
	- terraform plan and apply (see note below)
	- when Terraform applying, either use a secret.tfvars file saved in terraform folder, or input the database_info and warehouse_info manually. Note slightly different formats required - put an example below.

Note: for both local and Actions-based deployments, bucket names may need to be changed as AWS buckets must have unique names. List all instances of each bucket within file architecture.

NOTE: the database credentials are stored locally in a .tfvars file and gitignored. Get secret.tfvars file from colleague and run 'terraform apply -var-file="secret.tfvars"' command.

**How it Works**

1. Python functions and tests
2. Terraform code
3. Makefile
4. YAML file
- pay attention to MVP section of Joe's  [project specification](https://github.com/northcoders/de-project-specification) repo here.

**General notes**

use [this](https://www.markdownguide.org/cheat-sheet/) Markdown shortcut syntax guide to make it look prettier!