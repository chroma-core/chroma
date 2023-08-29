variable "project_id" {
  type = string
}
variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.5"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "machine_type" {
  type    = string
  default = "e2-small"
}
