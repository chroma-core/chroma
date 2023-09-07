variable "project" {
  type        = string
  description = "ID Google project"
}

variable "region" {
  type        = string
  description = "Region Google project"
  default     = "europe-north1"
}

variable "zone" {
  type        = string
  description = "Google Compute Engine zone"
  default     = "europe-north1-c"
}
  
variable "service" {
  type        = string
  description = "Name of data pipeline project to use as resource prefix"
  default      = "chroma-service"
}

variable "machine_type" {
  type        = string
  description = "Google Compute Engine machine type"
  default     = "e2-small"
}

variable "image" {
  type        = string
  description = "Image name inside Artifact Registry"
  default     = "chroma-db-server"

}

variable "registry" {
  type        = string
  description = "Registry name inside Artifact Registry"
  default     = "chroma-db-server-registry"
}


variable "service_account" {
  type        = string
  description = "Service account email with permission to Artifact Registry"
}

variable "disk_size" {
  type        = string
  description = "Attached disk size"
  default      = 10
}

variable "disk_type" {
  type        = string
  description = "Attached disk type"
  default      = "pd-ssd"
}