#
# This file contains the main resources necessary to configure and deploy the Lambda
#

locals {
  lambda_file = "../target/lambda/hello-delta-rust/bootstrap.zip"
}

resource "aws_lambda_function" "hello" {
  description      = "A simple Lambda that says hello with Delta metadata"
  filename         = local.lambda_file
  function_name    = "hello-delta-rust"
  source_code_hash = filebase64sha256(local.lambda_file)
  role             = aws_iam_role.iam_for_lambda.arn
  handler          = "provided"
  runtime          = "provided.al2"

  environment {
    variables = {
      RUST_LOG  = "deltalake=info,hello-delta-rust=debug"
      TABLE_URL = "s3://${aws_s3_bucket.data.bucket}/COVID-19_NYT"
    }
  }
}

resource "aws_lambda_function_url" "hello" {
  function_name      = aws_lambda_function.hello.function_name
  authorization_type = "NONE"
}

output "lambda_function_url" {
  value = aws_lambda_function_url.hello.function_url
}

resource "aws_s3_bucket" "data" {
  # Generating a random bucket name to ensure readers don't have conflicts!
  bucket = "deltalake-examples-${random_id.random.hex}"
}

# Upload the test Delta Table
resource "aws_s3_object" "covid19" {
  for_each = fileset("../../../../datasets/COVID-19_NYT/", "**")
  bucket   = aws_s3_bucket.data.id
  key      = "COVID-19_NYT/${each.value}"
  source   = "../../../../datasets/COVID-19_NYT/${each.value}"
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
  name = "lambda-permissions"
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
  name               = "iam_for_rust_lambda"
  assume_role_policy = data.aws_iam_policy_document.assume_role.json
  managed_policy_arns = [
    aws_iam_policy.lambda_permissions.arn,
  ]
}
