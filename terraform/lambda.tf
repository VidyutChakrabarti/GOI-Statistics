resource "aws_lambda_function" "csv_to_rds" {
  function_name = "csv_to_rds"
  role          = aws_iam_role.lambda_role.arn
  runtime       = "python3.9"
  handler       = "lambda_function.lambda_handler"
  filename      = "lambda_function.zip"
  
  # Attach a public Lambda layer for psycopg2 (by Klayers for Python 3.9)
  layers = [
    "arn:aws:lambda:us-east-1:770693421928:layer:Klayers-p39-psycopg2:1"
  ]

  environment {
    variables = {
      DB_HOST    = aws_db_instance.rds_instance.endpoint,
      DB_NAME    = var.rds_db_name,
      DB_USER    = "admin",
      DB_PASSWORD= var.rds_password,
      S3_BUCKET  = var.s3_bucket_name
    }
  }

  tags = {
    Name = "CSVtoRDSLambda"
  }
}
