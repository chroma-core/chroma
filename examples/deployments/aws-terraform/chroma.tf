terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Define provider
variable "AWS_ACCESS_KEY" {}
variable "AWS_SECRET_ACCESS_KEY" {}

provider "aws" {
  access_key = var.AWS_ACCESS_KEY
  secret_key = var.AWS_SECRET_ACCESS_KEY
  region     = var.region
}

# Define variables
variable "instance_count" {
  description = "Number of instances in the cluster"
  default     = 1
}

# Create security group
resource "aws_security_group" "chroma_sg" {
  name        = "chroma-cluster-sg"
  description = "Security group for the cluster nodes"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  dynamic "ingress"  {
    for_each = var.public_access ? [1] : []
    content {
      from_port   = 8000
      to_port     = 8000
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }
#
#  ingress {
#    from_port        = 0
#    to_port          = 0
#    protocol         = "-1"
#    self             = true
#  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_key_pair" "chroma-keypair" {
  key_name   = "chroma-keypair"  # Replace with your desired key pair name
  public_key = file(var.ssh_public_key)  # Replace with the path to your public key file
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}
# Create EC2 instances
resource "aws_instance" "cluster_nodes" {
  count           = var.instance_count
  ami             = data.aws_ami.ubuntu.id
  instance_type   = var.instance_type
  key_name        = "chroma-keypair"
  security_groups = [aws_security_group.chroma_sg.name]

  user_data = templatefile("${path.module}/startup.sh", {
    chroma_release = var.chroma_release,
    enable_auth = var.enable_auth,
    basic_auth_credentials = var.basic_auth_credentials,
  })

  tags = {
    Name = "chroma-cluster-node-${count.index}"
  }

}

output "instance_public_ip" {
  value = aws_instance.cluster_nodes.*.public_ip[0]
}

output "instance_private_ip" {
  value = aws_instance.cluster_nodes.*.private_ip[0]
}
