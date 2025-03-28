provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}

resource "aws_s3_bucket" "tf_state" {
  bucket = var.state_bucket_name
  acl    = "private"

  tags = {
    Name = var.state_bucket_name
  }
}

# Use the aws_s3_bucket_versioning resource to enable versioning
resource "aws_s3_bucket_versioning" "tf_state_versioning" {
  bucket = aws_s3_bucket.tf_state.id

  versioning_configuration {
    status = "Enabled"
  }
}
