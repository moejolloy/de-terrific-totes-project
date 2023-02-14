ingest bucket named ingest-bucket-totedd-1402
processed bucket named processed-bucket-totedd-1402

required_version = "~> 1.3.7" in backend.tf means must have terraform 1.3.7 to 1.3.9

Ingest Lambda has permission to read and write to ingest bucket
Processed Lambda has permission to read only from ingest bucket
Processed Lambda has permission to read and write to processed bucket

Each have their own roles