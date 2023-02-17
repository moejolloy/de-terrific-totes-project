data "aws_iam_policy_document" "read-write-access-ingest-bucket-document" {
  statement {
    sid       = "ListObjectsInBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.ingest-bucket.bucket}"]
  }
  statement {
    sid       = "AllObjectActions"
    effect    = "Allow"
    actions   = ["s3:*Object"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.ingest-bucket.bucket}/*"]
  }
}

data "aws_iam_policy_document" "read-only-access-ingest-bucket-document" {
  statement {
    sid       = "ListObjectsInBucket"
    effect    = "Allow"
    actions   = ["s3:Get*", "s3:List*"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.ingest-bucket.bucket}"]
  }
}

data "aws_iam_policy_document" "read-write-access-processed-bucket-document" {
  statement {
    sid       = "ListObjectsInBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.processed-bucket.bucket}"]
  }
  statement {
    sid       = "AllObjectActions"
    effect    = "Allow"
    actions   = ["s3:*Object"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.processed-bucket.bucket}/*"]
  }
}

data "aws_iam_policy_document" "read-only-access-processed-bucket-document" {
  statement {
    sid       = "ListObjectsInBucket"
    effect    = "Allow"
    actions   = ["s3:Get*", "s3:List*"]
    resources = ["arn:aws:s3:::${aws_s3_bucket.processed-bucket.bucket}"]
  }
}

resource "aws_iam_policy" "read-write-access-ingest-bucket-policy" {
  name_prefix = "read-write-access-ingest-bucket-policy"
  policy      = data.aws_iam_policy_document.read-write-access-ingest-bucket-document.json
}

resource "aws_iam_policy" "read-only-access-ingest-bucket-policy" {
  name_prefix = "read-only-access-ingest-bucket-policy"
  policy      = data.aws_iam_policy_document.read-only-access-ingest-bucket-document.json
}

resource "aws_iam_policy" "read-write-access-processed-bucket-policy" {
  name_prefix = "read-write-access-processed-bucket-policy"
  policy      = data.aws_iam_policy_document.read-write-access-processed-bucket-document.json
}

resource "aws_iam_policy" "read-only-access-processed-bucket-policy" {
  name_prefix = "read-only-access-processed-bucket-policy"
  policy      = data.aws_iam_policy_document.read-only-access-processed-bucket-document.json
}

resource "aws_iam_role" "ingest-lambda-role" {
  name               = "ingest-lambda-role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role" "processed-lambda-role" {
  name               = "processed-lambda-role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role" "populate-lambda-role" {
  name               = "populate-lambda-role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

resource "aws_iam_role_policy_attachment" "ingest-lambda-read-write-policy-attachment" {
  role       = aws_iam_role.ingest-lambda-role.name
  policy_arn = aws_iam_policy.read-write-access-ingest-bucket-policy.arn
}

resource "aws_iam_role_policy_attachment" "processed-lambda-read-only-ingest-bucket-policy-attachment" {
  role       = aws_iam_role.processed-lambda-role.name
  policy_arn = aws_iam_policy.read-only-access-ingest-bucket-policy.arn
}

resource "aws_iam_role_policy_attachment" "processed-lambda-read-write-processed-bucket-policy-attachment" {
  role       = aws_iam_role.processed-lambda-role.name
  policy_arn = aws_iam_policy.read-write-access-processed-bucket-policy.arn
}

resource "aws_iam_role_policy_attachment" "populate-lambda-read-only-processed-bucket-policy-attachment" {
  role       = aws_iam_role.populate-lambda-role.name
  policy_arn = aws_iam_policy.read-only-access-processed-bucket-policy.arn
}
