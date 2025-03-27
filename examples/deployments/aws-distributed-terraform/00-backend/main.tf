terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # First, run:
  #   terraform init && terraform apply
  #
  # Then, uncomment the below block, replace NAME and REGION with your values, and run:
  #   terraform init -reconfigure
  #
  # Type "yes" when prompted to migrate the state to the s3 bucket.

  # backend "s3" {
  #   bucket         = "MY_ACCOUNT_NAME-terraform-state"
  #   key            = "backend/terraform.tfstate"
  #   region         = "MY_REGION"
  #   dynamodb_table = "MY_ACCOUNT_NAME-terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "terraform_state" {
  bucket = "${var.account_name}-terraform-state"
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "enabled" {
  bucket = aws_s3_bucket.terraform_state.bucket
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "default" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "public_access" {
  bucket                  = aws_s3_bucket.terraform_state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_dynamodb_table" "terraform_locks" {
  name         = "${var.account_name}-terraform-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}
