variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}


variable "aws_access_key" {
  description = "AWS access key"
  type        = string
  sensitive   = true
}


variable "aws_secret_key" {
  description = "AWS secret key"
  type        = string
  sensitive   = true
}


variable "state_bucket_name" {
  description = "The name of the S3 bucket to be used for Terraform remote state"
  type        = string
}
