variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.12"
}

#TODO this should be updated to point to https://raw.githubusercontent.com/chroma-core/chroma/main/examples/deployments/common/startup.sh in the repo
data "http" "startup_script_remote" {
  url = "https://raw.githubusercontent.com/chroma-core/chroma/main/examples/deployments/common/startup.sh"
}

data "template_file" "user_data" {
  template = data.http.startup_script_remote.response_body

  vars = {
    chroma_release         = var.chroma_release
    enable_auth            = var.enable_auth
    auth_type              = var.auth_type
    basic_auth_credentials = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}"
    token_auth_credentials = random_password.chroma_token.result
  }
}

variable "region" {
  description = "AWS Region"
  type        = string
  default     = "us-west-1"
}

variable "instance_type" {
  description = "AWS EC2 Instance Type"
  type        = string
  default     = "t3.medium"
}


variable "public_access" {
  description = "Enable public ingress on port 8000"
  type        = bool
  default     = true // or true depending on your needs
}

variable "enable_auth" {
  description = "Enable authentication"
  type        = bool
  default     = true // or false depending on your needs
}

variable "auth_type" {
  description = "Authentication type"
  type        = string
  default     = "token" // or token depending on your needs
  validation {
    condition     = contains(["basic", "token"], var.auth_type)
    error_message = "The auth type must be either basic or token"
  }
}

resource "random_password" "chroma_password" {
  length  = 16
  special = true
  lower   = true
  upper   = true
}

resource "random_password" "chroma_token" {
  length  = 32
  special = false
  lower   = true
  upper   = true
}


locals {
  basic_auth_credentials = {
    username = "chroma"
    password = random_password.chroma_password.result
  }
  token_auth_credentials = {
    token = random_password.chroma_token.result
  }
  tags = {
    Name = "chroma",
    Release = "release-${replace(var.chroma_release, ".", "")}",
  }
}

variable "ssh_public_key" {
  description = "SSH Public Key"
  type        = string
  default     = "./chroma-aws.pub"
}
variable "ssh_private_key" {
  description = "SSH Private Key"
  type        = string
  default     = "./chroma-aws"
}

variable "chroma_instance_volume_size" {
  description = "The size of the instance volume - the root volume"
  type        = number
  default     = 30
}

variable "chroma_data_volume_size" {
  description = "EBS Volume Size of the attached data volume where your chroma data is stored"
  type        = number
  default     = 20
}

variable "chroma_data_volume_snapshot_before_destroy" {
  description = "Take a snapshot of the chroma data volume before destroying it"
  type        = bool
  default     = false
}

variable "chroma_data_restore_from_snapshot_id" {
  description = "Restore the chroma data volume from a snapshot"
  type        = string
  default     = ""
}

variable "chroma_port" {
  default     = "8000"
  description = "The port that chroma listens on"
  type        = string
}

variable "source_ranges" {
  default     = ["0.0.0.0/0"]
  type        = list(string)
  description = "List of CIDR ranges to allow through the firewall"
}

variable "mgmt_source_ranges" {
  default     = ["0.0.0.0/0"]
  type        = list(string)
  description = "List of CIDR ranges to allow for management of the Chroma instance. This is used for SSH incoming traffic filtering"
}
