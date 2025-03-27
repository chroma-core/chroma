terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment the following, and replace:
  #   MY_ACCOUNT_NAME: your AWS account name
  #   MY_REGION: your AWS region

  # backend "s3" {
  #   bucket         = "MY_ACCOUNT_NAME-terraform-state"
  #   key            = "network/terraform.tfstate"
  #   region         = "MY_REGION"
  #   dynamodb_table = "MY_ACCOUNT_NAME-terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

locals {
  azs = slice(sort(data.aws_availability_zones.available.names), 0, 3)
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.name}-vpc"

  cidr = var.vpc_cidr
  azs  = local.azs

  # Generate ranges for private/public subnets based on the provided CIDR.
  # For example, given vpc_cidr of 10.0.0.0/17, this will produce:
  # private_subnets:
  #   - 10.0.0.0/20
  #   - 10.0.16.0/20
  #   - 10.0.32.0/20
  # public_subnets:
  #   - 10.0.48.0/20
  #   - 10.0.64.0/20
  #   - 10.0.80.0/20
  private_subnets = [ for k, v in local.azs : cidrsubnet(var.vpc_cidr, 3, k) ]
  public_subnets  = [ for k, v in local.azs : cidrsubnet(var.vpc_cidr, 3, k + 3) ]

  enable_nat_gateway = true
  single_nat_gateway = true
}
