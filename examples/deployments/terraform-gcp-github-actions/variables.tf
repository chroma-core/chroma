locals {
  labels = {
    "service" = var.service
  }
}

variable "project" {
  type        = string
  description = "ID Google project"
}

variable "region" {
  type        = string
  description = "Region Google project"
}

variable "service" {
  type        = string
  description = "Name of data pipeline project to use as resource prefix"
}

variable "image" {
  type        = string
  description = "Image name inside Artifact Registry"
}

variable "registry" {
  type        = string
  description = "Registry name inside Artifact Registry"
}


variable "service_account" {
  type        = string
  description = "Service account email with permission to Artifact Registry"
}
