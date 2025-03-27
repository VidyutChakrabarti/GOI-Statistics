variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Globally unique S3 bucket name for CSV files"
  type        = string
}

variable "rds_db_name" {
  description = "Database name for the RDS instance"
  type        = string
  default     = "householddb"
}

variable "rds_username" {
  description = "Username for the RDS instance"
  type        = string
  default     = "adminuser"
}

variable "rds_password" {
  description = "Password for the RDS instance"
  type        = string
  sensitive   = true
}
