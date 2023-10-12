variable "chroma_image_reg_url" {
  description = "The URL of the chroma-core image registry (e.g. docker.io/chromadb/chroma). The URL must also include the image itself without the tag."
  type        = string
  default     = "docker.io/chromadb/chroma"
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
    condition     = contains([ "token"], var.auth_type)
    error_message = "Only token is supported as auth type"
  }
}

resource "random_password" "chroma_token" {
  length  = 32
  special = false
  lower   = true
  upper   = true
}


locals {
  token_auth_credentials = {
    token = random_password.chroma_token.result
  }
}

variable "chroma_data_volume_size" {
  description = "The size of the attached data volume in GB."
  type        = number
  default     = 20
}

variable "chroma_data_volume_device_name" {
  default     = "chroma-disk-0"
  description = "The device name of the chroma data volume"
  type        = string
}

variable "chroma_data_volume_mount_path" {
  default     = "/chroma-data"
  description = "The mount path of the chroma data volume"
  type        = string
}
