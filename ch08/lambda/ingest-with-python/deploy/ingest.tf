#
# This file contains the main resources necessary to configure and deploy the Lambda

locals {
  lambda_file = "../target/lambda_function.zip"
  layer_file  = "../target/lambda_layer.zip"
}

resource "aws_lambda_function" "ingest" {
  description      = "An HTTP Lambda that ingests JSON arrays of data"
  function_name    = "ingest-json-python"
  filename         = local.lambda_file
  source_code_hash = filebase64sha256(local.lambda_file)
  role             = aws_iam_role.iam_for_lambda.arn
  memory_size      = 512
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.10"
  timeout          = 60
  # Using the AWS SDK Pandas layer as well, since that provides pyarrow and pandas
  layers = [
    aws_lambda_layer_version.python.arn,
    "arn:aws:lambda:us-west-2:336392948345:layer:AWSSDKPandas-Python310:8"
  ]

  environment {
    variables = {
      RUST_LOG                   = "deltalake=info"
      TABLE_URL                  = "s3://${aws_s3_bucket.data.bucket}/users"
      AWS_S3_ALLOW_UNSAFE_RENAME = true
    }
  }
}

# Set to 1 since AWS_S3_ALLOW_UNSAFE_RENAME is set
#resource "aws_lambda_provisioned_concurrency_config" "ingest" {
#  function_name                     = aws_lambda_function.ingest.function_name
#  provisioned_concurrent_executions = 1
#  qualifier                         = aws_lambda_function.ingest.version
#}

resource "aws_s3_object" "layer" {
  bucket = aws_s3_bucket.data.id
  key    = "lambda_layer.zip"
  etag   = filebase64sha256(local.layer_file)
  source = local.layer_file
}
resource "aws_lambda_layer_version" "python" {
  layer_name          = "python_with_delta"
  s3_bucket           = aws_s3_bucket.data.id
  s3_key              = aws_s3_object.layer.key
  s3_object_version   = aws_s3_object.layer.version_id
  compatible_runtimes = ["python3.10"]
}

resource "aws_lambda_function_url" "ingest" {
  function_name      = aws_lambda_function.ingest.function_name
  authorization_type = "NONE"
}

output "lambda_function_url" {
  value = aws_lambda_function_url.ingest.function_url
}

resource "aws_s3_bucket" "data" {
  # Generating a random bucket name to ensure readers don't have conflicts!
  bucket = "deltalake-examples-ingest-json-${random_id.random.hex}"
}

# Upload the test Delta Table
resource "aws_s3_object" "covid19" {
  for_each = fileset("../tests/fixture", "**")
  bucket   = aws_s3_bucket.data.id
  key      = "users/${each.value}"
  source   = "../tests/fixture/${each.value}"
}


resource "random_id" "random" {
  byte_length = 8
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = [
      "sts:AssumeRole",
    ]
  }
}

resource "aws_iam_policy" "lambda_permissions" {
  name = "python-ingest-lambda-permissions"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:*"]
        Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
        Effect   = "Allow"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams",
        ]
        Resource = "arn:aws:logs:*:*:*",
        Effect   = "Allow"
      },

    ]
  })
}

resource "aws_iam_role" "iam_for_lambda" {
  name               = "iam_for_python_ingest_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  managed_policy_arns = [
    aws_iam_policy.lambda_permissions.arn,
  ]
}
