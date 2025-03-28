output "rds_endpoint" {
  description = "The endpoint of the RDS instance."
  value       = aws_db_instance.rds_instance.endpoint
}

output "s3_bucket_name" {
  description = "The name of the S3 bucket where CSV files are stored."
  value       = aws_s3_bucket.csv_bucket.id
}
