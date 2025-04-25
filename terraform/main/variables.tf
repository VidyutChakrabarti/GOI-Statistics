variable "aws_region" {
  description = "AWS region to deploy resources."
  type        = string
  default     = "us-east-1"
}


variable "aws_access_key" {
  description = "AWS access key."
  type        = string
  sensitive   = true
}


variable "aws_secret_key" {
  description = "AWS secret key."
  type        = string
  sensitive   = true
}


variable "s3_bucket_name" {
  description = "Name of the S3 bucket for CSV files."
  type        = string
}


variable "rds_password" {
  description = "Password for the RDS PostgreSQL instance."
  type        = string
  sensitive   = true
}


variable "rds_instance_class" {
  description = "RDS instance class (free-tier eligible: db.t2.micro)."
  type        = string
  default     = "db.t2.micro"
}


variable "rds_db_name" {
  description = "Database name for the RDS instance."
  type        = string
  default     = "household_db"
}


variable "vpc_cidr" {
  description = "CIDR block for the VPC."
  type        = string
  default     = "10.0.0.0/16"
}


variable "public_subnet_cidr" {
  description = "CIDR block for the public subnet."
  type        = string
  default     = "10.0.1.0/24"
}


variable "public_subnet_cidr_2" {
  description = "CIDR block for the secondary public subnet"
  type        = string
  default     = "10.0.2.0/24"
}


variable "availability_zone" {
  description = "Availability zone for resource deployment."
  type        = string
  default     = "us-east-1a"
}


variable "availability_zone_2" {
  description = "Availability zone for the secondary subnet"
  type        = string
  default     = "us-east-1b"
}


variable "allowed_ip" {
  description = "CIDR from which RDS allows inbound PostgreSQL access (use your IP for tighter security)."
  type        = string
  default     = "0.0.0.0/0"
}
