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

resource "aws_iam_policy" "read-write-access-ingest-bucket-policy" {
  name_prefix = "read-write-access-ingest-bucket-policy"
  policy      = data.aws_iam_policy_document.read-write-access-ingest-bucket-document.json
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

resource "aws_iam_role_policy_attachment" "ingest-lambda-read-write-policy-attachment" {
  role       = aws_iam_role.ingest-lambda-role.name
  policy_arn = aws_iam_policy.read-write-access-ingest-bucket-policy.arn
}
