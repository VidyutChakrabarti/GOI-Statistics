provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "tf_state" {
  bucket = var.state_bucket_name
  acl    = "private"

  versioning {
    enabled = true
  }

  tags = {
    Name = var.state_bucket_name
  }
}
