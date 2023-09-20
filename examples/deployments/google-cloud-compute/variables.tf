variable "project_id" {
  type        = string
  description = "The project id to deploy to"
}
variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.9"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "image" {
  default     = "debian-cloud/debian-11"
  description = "The image to use for the instance"
  type        = string
}

variable "vm_user" {
  default     = "debian"
  description = "The user to use for connecting to the instance. This is usually the default image user"
  type        = string
}

variable "machine_type" {
  type    = string
  default = "e2-small"
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
  tags = [
    "chroma",
    "release-${replace(var.chroma_release, ".", "")}",
  ]
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
