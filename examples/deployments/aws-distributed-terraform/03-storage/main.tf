locals {
  sysdb_user = "sysdb"
  sysdb_db = "sysdb"
  logdb_user = "logdb"
  logdb_db = "logdb"
}

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }

  # Uncomment the following, and replace:
  #   MY_ACCOUNT_NAME: your AWS account name
  #   MY_REGION: your AWS region

  # backend "s3" {
  #   bucket         = "MY_ACCOUNT_NAME-terraform-state"
  #   key            = "storage/terraform.tfstate"
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

# Uncomment the following, and replace:
#   MY_ACCOUNT_NAME: your AWS account name
#   MY_REGION: your AWS region

# data "terraform_remote_state" "compute" {
#   backend = "s3"
#   config = {
#     bucket = "MY_ACCOUNT_NAME-terraform-state"
#     key    = "compute/terraform.tfstate"
#     region = "MY_REGION"
#   }
# }

provider "aws" {
  region = var.region
}

data "aws_eks_cluster_auth" "default" {
  name = var.name
}

provider "kubernetes" {
  host                   = data.terraform_remote_state.compute.outputs.eks_cluster_endpoint
  cluster_ca_certificate = base64decode(data.terraform_remote_state.compute.outputs.eks_cluster_certificate_authority_data)
  token                  = data.aws_eks_cluster_auth.default.token
}

resource "aws_db_subnet_group" "db_subnet_group" {
  name       = "${var.name}-db-subnet-group"
  subnet_ids = data.terraform_remote_state.network.outputs.vpc_private_subnets
}

resource "aws_security_group" "db_security_group" {
  name        = "${var.name}-db-security-group"
  description = "Security group for ${var.name} database instances."
  vpc_id      = data.terraform_remote_state.network.outputs.vpc_id

  ingress {
    description = "Allow PostgreSQL traffic from the VPC CIDR block."
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [data.terraform_remote_state.network.outputs.vpc_cidr]
  }
}

resource "random_password" "sysdb_password" {
  length  = 48
  special = false
}

resource "aws_rds_cluster" "sysdb" {
  cluster_identifier              = "${var.name}-sysdb"
  engine                          = "aurora-postgresql"
  engine_version                  = "15.7"
  database_name                   = local.sysdb_db
  master_username                 = local.sysdb_user
  master_password                 = random_password.sysdb_password.result
  db_subnet_group_name            = aws_db_subnet_group.db_subnet_group.name
  vpc_security_group_ids          = [aws_security_group.db_security_group.id]
  availability_zones              = data.terraform_remote_state.network.outputs.vpc_azs
  skip_final_snapshot             = true
}

resource "aws_rds_cluster_instance" "sysdb_writer_instance" {
  count                        = 1
  engine                       = "aurora-postgresql"
  engine_version               = "15.7"
  cluster_identifier           = aws_rds_cluster.sysdb.id
  instance_class               = "db.t3.medium"
  identifier_prefix            = "${var.name}-sysdb-writer-instance"
  db_subnet_group_name         = aws_db_subnet_group.db_subnet_group.name
  performance_insights_enabled = true
}

resource "random_password" "logdb_password" {
  length  = 48
  special = false
}

resource "aws_rds_cluster" "logdb" {
  cluster_identifier     = "${var.name}-logdb"
  engine                 = "aurora-postgresql"
  engine_version         = "15.7"
  database_name          = local.logdb_db
  master_username        = local.logdb_user
  master_password        = random_password.logdb_password.result
  db_subnet_group_name   = aws_db_subnet_group.db_subnet_group.name
  vpc_security_group_ids = [aws_security_group.db_security_group.id]
  availability_zones     = data.terraform_remote_state.network.outputs.vpc_azs
  skip_final_snapshot    = true
}

resource "aws_rds_cluster_instance" "logdb_instances" {
  count                        = 1
  engine                       = "aurora-postgresql"
  engine_version               = "15.7"
  cluster_identifier           = aws_rds_cluster.logdb.id
  instance_class               = "db.t3.medium"
  identifier_prefix            = "${var.name}-logdb-instance"
  db_subnet_group_name         = aws_db_subnet_group.db_subnet_group.name
  performance_insights_enabled = true
}

resource "kubernetes_namespace" "chroma_namespace" {
  metadata {
    name = var.namespace
  }
}

module "compaction-service-iam-role-assumption" {
  source               = "bigdatabr/kubernetes-iamserviceaccount/aws"
  version              = "~> 1.0"
  cluster_name         = var.name
  namespace            = var.namespace
  role_name            = "compaction-service-role"
  service_account_name = "compaction-service-serviceaccount"
}

module "query-service-iam-role-assumption" {
  source               = "bigdatabr/kubernetes-iamserviceaccount/aws"
  version              = "~> 1.0"
  cluster_name         = var.name
  namespace            = var.namespace
  role_name            = "query-service-role"
  service_account_name = "query-service-serviceaccount"
}

resource "aws_s3_bucket" "chroma_storage_bucket" {
  bucket = var.bucket_name

  tags = {
    Name = var.bucket_name
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "chroma_storage_lifecycle" {
  bucket = aws_s3_bucket.chroma_storage_bucket.bucket

  rule {
    id     = "remove-incomplete-multipart-uploads"
    status = "Enabled"
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "chroma_storage_ownership" {
  bucket = aws_s3_bucket.chroma_storage_bucket.bucket
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_policy" "chroma_storage_bucket_policy" {
  bucket = aws_s3_bucket.chroma_storage_bucket.bucket

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          "AWS" = "${module.compaction-service-iam-role-assumption.iam_role.arn}"
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
        ]
        Resource = "arn:aws:s3:::${aws_s3_bucket.chroma_storage_bucket.bucket}/*"
      },
      {
        Effect = "Allow"
        Principal = {
          "AWS" = "${module.query-service-iam-role-assumption.iam_role.arn}"
        }
        Action = [
          "s3:GetObject",
        ]
        Resource = "arn:aws:s3:::${aws_s3_bucket.chroma_storage_bucket.bucket}/*"
      }
    ]
  })
}
