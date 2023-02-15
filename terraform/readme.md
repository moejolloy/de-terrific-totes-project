ingest bucket named ingest-bucket-totedd-1402
processed bucket named processed-bucket-totedd-1402

required_version = "~> 1.3.7" in backend.tf means must have terraform 1.3.7 to 1.3.9

Ingest Lambda has permission to read and write to ingest bucket
Processed Lambda has permission to read only from ingest bucket
Processed Lambda has permission to read and write to processed bucket
Populate Lambda has permission to read only from processed bucket

all lambda function names should refer to their appropriate variable in the vars.tf file.

Each have their own roles
