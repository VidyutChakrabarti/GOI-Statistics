output "s3_bucket_name" {
  description = "The name of the S3 bucket"
  value       = aws_s3_bucket.csv_bucket.bucket
}

output "rds_endpoint" {
  description = "The endpoint of the RDS instance"
  value       = aws_db_instance.rds_instance.address
}

output "lambda_function_name" {
  description = "The Lambda function name"
  value       = aws_lambda_function.csv_processor.function_name
}
