variable "chroma_core_repo_url" {
    description = "The URL of the chroma-core repository"
    type        = string
    default     = "https://github.com/chroma-core/chroma"
}

variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.13"
}

variable "region" {
  type    = string
  default = "oregon"
}

variable "render_plan" {
  default     = "starter"
  description = "The Render plan to use. This determines the size of the machine. NOTE: Terraform Render provider uses Render's API which requires at least starter plan."
  type = string
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
  tags = [
    "chroma",
    "release-${replace(var.chroma_release, ".", "")}",
  ]
}

variable "chroma_data_volume_size" {
  description = "Volume Size of the attached data volume where your chroma data is stored"
  type        = number
  default     = 20
}

variable "chroma_data_volume_device_name" {
  default     = "chroma-disk-0"
  description = "The device name of the chroma data volume"
  type        = string
}
