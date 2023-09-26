variable "instance_image" {
    description = "The image to use for the instance"
    type        = string
    default     = "ubuntu-22-04-x64"
}
variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.12"
}

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
  description = "DO Region"
  type        = string
  default     = "nyc2"
}

variable "instance_type" {
  description = "Droplet size"
  type        = string
  default     = "s-2vcpu-4gb"
}


variable "public_access" {
  description = "Enable public ingress on port 8000"
  type        = bool
  default     = true // or false depending on your needs
}

variable "enable_auth" {
  description = "Enable authentication"
  type        = bool
  default     = true // or false depending on your needs
}

variable "auth_type" {
  description = "Authentication type"
  type        = string
  default     = "token" // or basic depending on your needs
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
  tags = [
    "chroma",
    "release-${replace(var.chroma_release, ".", "")}",
  ]
}

variable "ssh_public_key" {
  description = "SSH Public Key"
  type        = string
  default     = "./chroma-do.pub"
}
variable "ssh_private_key" {
  description = "SSH Private Key"
  type        = string
  default     = "./chroma-do"
}

variable "chroma_data_volume_size" {
  description = "EBS Volume Size of the attached data volume where your chroma data is stored"
  type        = number
  default     = 20
}


variable "chroma_port" {
  default     = "8000"
  description = "The port that chroma listens on"
  type        = string
}

variable "source_ranges" {
  default     = ["0.0.0.0/0", "::/0"]
  type        = list(string)
  description = "List of CIDR ranges to allow through the firewall"
}

variable "mgmt_source_ranges" {
  default     = ["0.0.0.0/0", "::/0"]
  type        = list(string)
  description = "List of CIDR ranges to allow for management of the Chroma instance. This is used for SSH incoming traffic filtering"
}
