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

  dynamic "ingress" {
    for_each = var.public_access ? [1] : []
    content {
      from_port   = 8000
      to_port     = 8000
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "chroma"
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
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  owners = ["099720109477"] # Canonical
}
# Create EC2 instances
resource "aws_instance" "chroma_instance" {
  ami             = data.aws_ami.ubuntu.id
  instance_type   = var.instance_type
  key_name        = "chroma-keypair"
  security_groups = [aws_security_group.chroma_sg.name]

  user_data = templatefile("${path.module}/startup.sh", {
    chroma_release         = var.chroma_release,
    enable_auth            = var.enable_auth,
    auth_type              = var.auth_type,
    basic_auth_credentials = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}",
    token_auth_credentials = random_password.chroma_token.result,
  })

  tags = {
    Name = "chroma"
  }

  ebs_block_device {
    device_name = "/dev/sda1"
    volume_size = var.chroma_instance_volume_size  # size in GBs
  }
}


resource "aws_ebs_volume" "chroma-volume" {
  availability_zone = aws_instance.chroma_instance.availability_zone
  size              = var.chroma_data_volume_size

  tags = {
    Name = "chroma"
  }

  lifecycle {
    prevent_destroy = var.prevent_chroma_data_volume_delete # size in GBs
  }
}

locals {
  cleaned_volume_id = replace(aws_ebs_volume.chroma-volume.id, "-", "")
}

resource "aws_volume_attachment" "chroma_volume_attachment" {
  device_name = "/dev/sdh"
  volume_id   = aws_ebs_volume.chroma-volume.id
  instance_id = aws_instance.chroma_instance.id
  provisioner "remote-exec" {
    inline = [
      "export VOLUME_ID=${local.cleaned_volume_id} && sudo mkfs -t ext4 /dev/$(lsblk -o +SERIAL | grep $VOLUME_ID | awk '{print $1}')",
      "sudo mkdir /chroma-data",
      "export VOLUME_ID=${local.cleaned_volume_id} && sudo mount /dev/$(lsblk -o +SERIAL | grep $VOLUME_ID | awk '{print $1}') /chroma-data"
    ]

    connection {
      host = aws_instance.chroma_instance.public_ip
      type = "ssh"
      user = "ubuntu"
      private_key = file(var.ssh_private_key)
    }
  }
    depends_on = [aws_instance.chroma_instance, aws_ebs_volume.chroma-volume]
}


output "instance_public_ip" {
  value = aws_instance.chroma_instance.public_ip
}

output "instance_private_ip" {
  value = aws_instance.chroma_instance.private_ip
}

output "chroma_auth_token" {
  value = random_password.chroma_token.result
  sensitive = true
}


output "chroma_auth_basic" {
  value = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}"
  sensitive = true
}
