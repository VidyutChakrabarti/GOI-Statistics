terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  
  backend "s3" {
    bucket = "vidyut-terraform-state-bucket-123"  # The bucket created by the bootstrap module.
    key    = "terraform.tfstate"
    region = "us-east-1"
  }
}


provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
}
