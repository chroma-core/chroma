variable "chroma_core_repo_url" {
    description = "The URL of the chroma-core repository"
    type        = string
    default     = "https://github.com/chroma-core/chroma"
}

variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.12"
}

variable "region" {
  type    = string
  default = "oregon"
}

variable "render_plan" {
  default     = "starter"
  description = "The Render plan to use. This determines the size of the machine."
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

variable "prevent_chroma_data_volume_delete" {
  description = "Prevent the chroma data volume from being deleted when the instance is terminated"
  type        = bool
  default     = false
}

variable "disk_type" {
  default     = "pd-ssd"
  description = "The type of disk to use for the instance. Can be either pd-standard or pd-ssd"
}

variable "labels" {
  default = {
    environment = "dev"
  }
  description = "Labels to apply to all resources in this example"
  type        = map(string)
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
