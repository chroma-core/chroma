locals {
  eks_cluster_version = "1.32"
}

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
  #   key            = "compute/terraform.tfstate"
  #   region         = "MY_REGION"
  #   dynamodb_table = "MY_ACCOUNT_NAME-terraform-locks"
  #   encrypt        = true
  # }
}

# Uncomment the following, and replace:
#   MY_ACCOUNT_NAME: your AWS account name
#   MY_REGION: your AWS region

# data "terraform_remote_state" "network" {
#   backend = "s3"
#   config = {
#     bucket = "MY_ACCOUNT_NAME-terraform-state"
#     key    = "network/terraform.tfstate"
#     region = "MY_REGION"
#   }
# }

provider "aws" {
  region = var.region
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.name
  cluster_version = local.eks_cluster_version

  cluster_endpoint_public_access = true

  enable_cluster_creator_admin_permissions = true

  vpc_id     = data.terraform_remote_state.network.outputs.vpc_id
  subnet_ids = data.terraform_remote_state.network.outputs.vpc_private_subnets

  cluster_compute_config = {
    enabled    = true
    node_pools = ["general-purpose"]
  }

  cluster_addons = {
    coredns = {}
  }
}