# de-terrific-totes-project

![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Terraform](https://img.shields.io/badge/terraform-%235835CC.svg?style=for-the-badge&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)

![Build Status](https://img.shields.io/github/actions/workflow/status/moejolloy/de-terrific-totes-project/test-and-deploy.yml?event=push&style=for-the-badge)
![Code Size](https://img.shields.io/github/languages/code-size/moejolloy/de-terrific-totes-project?style=for-the-badge)
![Contributors](https://img.shields.io/github/contributors/moejolloy/de-terrific-totes-project?style=for-the-badge)
![Forks](https://img.shields.io/github/forks/moejolloy/de-terrific-totes-project?style=for-the-badge)

---

## Table of Contents
|Contents|
|----------|
|[Project Summary](#project-summary)|
|[Directions for Deployment](#directions-for-deployment)|
|[How it Works]()|

---

## Project Summary

A data platform that extracts data from an operational database, archives it in a data lake, and makes it availabale in a remodelled OLAP data warehouse.

---

The primary data source for the project is a database called `totesys` which is meant to simulate the back end data of a commercial application. The company `TerrificTotes` makes branded and customised tote bags for retail and corporate clients.
Fake data from the sales and purchasing teams is inserted and updated into this database several times a day.

The full ERD for the database is detailed [here](https://dbdiagram.io/d/6332fecf7b3d2034ffcaaa92).

In order to optimise data for analytical queries away from the operational systems, the data is remodelled into three overlapping star schemas:
 - ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
 - ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
 - ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting [data warehouse](https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca) is shown below:

<img
    src = 'complete-erd.png'
    alt = 'Complete entity relationship diagram for data warehouse'
    width = 650
    height = 500
    align = top
/>

---

[(Back to top)](#table-of-contents)

## Directions for Deployment

There are two primary ways of deploying the infrastructure and functionality contained within this repo. This section will take you through each method step-by-step.

1. Using GitHub Actions:
	- Fork the repository.
	- In your version of the repo on GitHub, from it's main page, click on Settings, then Secrets And Variables, then Actions.
    - In your forked version of the repo on Github, click on Settings → Secrets And Variables → Actions.
	- From the Actions and Secrets page, click New Repository Secret.
	- Create 4 repository secrets: your database credentials as DATABASE_CREDENTIALS, data warehouse credentials as DATA_WAREHOUSE_CREDENTIALS, AWS access key as AWS_ACCESS_KEY and AWS secret key as AWS_SECRET_KEY. Note: as repository owner, only you can change the value of these secrets in future.
	- The data warehouse and database credentials secrets must be formatted correctly to ensure they are successfully parsed as key/value pairs by AWS Secrets Manager. Format as follows:

	```json
	{ "host" : "somewhere-on-internet", "port" : "5432", "database" : "dummy", "user" : "dummy", "password" : "your-password" }
	```

	- Change the name of the bucket for storing the Terraform state file in backend.tf and in the YAML file. This will ensure a unique bucket is created.
	- On your forked repo page, click on the Actions menu, then on the Final Test and Deploy workflow. Next to the notice that this repo has a workflow dispatch, click on Run Workflow, from main branch and in dev environment.
	- In future, your code will redeploy when you push code to the main branch or trigger this dispatch again. You may wish to create branch protections and remove the workflow dispatch in the YAML file to ensure that this only happens with a degree of control.

2. Local Deployment:
	- Fork the repo and clone it to your local machine.
	- Install Terraform as outlined [here](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli).
	- In the root directory, set up a virtual environment with:

	```sh
	python -m venv venv
	```

	- Activate the environment using the following command:

		```sh
		source venv/bin/activate
		```

		- Note: this repo will require you to use Python 3.9.16 as instructed in the .python-version file.

	- In the repo's root directory, run the following command in your terminal:
	 	
	```sh
	make all
	```

	- This instructs the Makefile to run it's 'all' command, which will install all the dev requirements needed in the requirements.txt file. The runtime requirements are handled within the Terraform infrastructure.

	-  Use `aws configure` command, then enter in your AWS login details.
	-  Run `aws sts get-caller-identity` to check you are logged in correctly.
	- Apply the terraform state bucket creation command as follows (add a suffix to make sure the bucket name is unique):

		```sh
		aws s3 mb s3://terraform-state-bucket-totedd-<SUFFIX>
		```

	- set the name of the state bucket in the YAML file and Backend.tf file to match the bucket you have just created.
	- Apply `terraform init -reconfigure` to initialise terraform and use the state file bucket as a backend.
	- Terraform plan and apply (see note below)
	- When using both terraform plan and terraform apply commands, terraform will prompt you to provide the sensitive values database_info and warehouse_info. Either input these manually or use a secret.tfvars file saved in the terraform folder.

        The format required for AWS to parse the secrets as key/value pairs is as follows:
	
    	```
		database_info = "{ \"host\" : \"some-where-on-internet\", \"port\" : \"8686\", \"database\" : \"dummy\", \"user\" : \"dummy\", \"password\" : \"my-pass" }"
		```

[(Back to top)](#table-of-contents)

---

**How it Works**

1. Python functions and tests
2. Terraform code
3. Makefile
4. YAML file
- pay attention to MVP section of Joe's  [project specification](https://github.com/northcoders/de-project-specification) repo here.

**General notes**

use [this](https://www.markdownguide.org/cheat-sheet/) Markdown shortcut syntax guide to make it look prettier!