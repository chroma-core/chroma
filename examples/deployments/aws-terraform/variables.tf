variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.8"
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

variable "prevent_chroma_data_volume_delete" {
    description = "Prevent the chroma data volume from being deleted when the instance is terminated"
    type        = bool
    default     = false
}
