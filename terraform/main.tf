terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

# Data sources for default VPC and subnets
data "aws_vpc" "default" {
  default = true
}

data "aws_subnet_ids" "default" {
  vpc_id = data.aws_vpc.default.id
}

###########################
# S3 Bucket for CSV Files #
###########################

resource "aws_s3_bucket" "csv_bucket" {
  bucket        = var.s3_bucket_name
  acl           = "private"
  force_destroy = true
}

#############################################
# RDS PostgreSQL Instance
#############################################

resource "aws_db_instance" "rds_instance" {
  identifier              = "household-rds-instance"
  allocated_storage       = 20
  storage_type            = "gp2"
  engine                  = "postgres"
  engine_version          = "13.3"
  instance_class          = "db.t2.micro"
  name                    = var.rds_db_name
  username                = var.rds_username
  password                = var.rds_password
  parameter_group_name    = "default.postgres13"
  skip_final_snapshot     = true
  publicly_accessible     = true
  # For production, adjust security as needed.
}

#############################################
# Security Group: Allow Lambda to access RDS
#############################################

resource "aws_security_group" "lambda_to_rds" {
  name        = "lambda-to-rds-sg"
  description = "Allow Lambda to access RDS on port 5432"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

######################################
# IAM Role and Policy for Lambda
######################################

resource "aws_iam_role" "lambda_role" {
  name = "lambda_s3_rds_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "lambda_policy"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Effect   = "Allow",
        Resource = [
          aws_s3_bucket.csv_bucket.arn,
          "${aws_s3_bucket.csv_bucket.arn}/*"
        ]
      },
      {
        Action   = ["rds:DescribeDBInstances"],
        Effect   = "Allow",
        Resource = "*"
      },
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

######################################
# Lambda Function to Process CSV Files
######################################

resource "aws_lambda_function" "csv_processor" {
  function_name = "csv_to_rds_processor"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.8"
  filename      = "${path.module}/../lambda/lambda_function.zip"
  source_code_hash = filebase64sha256("${path.module}/../lambda/lambda_function.zip")

  environment {
    variables = {
      DB_HOST     = aws_db_instance.rds_instance.address
      DB_NAME     = var.rds_db_name
      DB_USER     = var.rds_username
      DB_PASSWORD = var.rds_password
      S3_BUCKET   = aws_s3_bucket.csv_bucket.bucket
    }
  }

  vpc_config {
    subnet_ids         = data.aws_subnet_ids.default.ids
    security_group_ids = [aws_security_group.lambda_to_rds.id]
  }
}

#############################################
# Allow S3 to Invoke the Lambda Function
#############################################

resource "aws_lambda_permission" "allow_s3_to_call_lambda" {
  statement_id  = "AllowS3InvokeLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.csv_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.csv_bucket.arn
}

#############################################
# S3 Bucket Notification to Trigger Lambda on CSV Upload
#############################################

resource "aws_s3_bucket_notification" "s3_event" {
  bucket = aws_s3_bucket.csv_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.csv_processor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.allow_s3_to_call_lambda]
}
