variable "chroma_release" {
  description = "The chroma release to deploy"
  type        = string
  default     = "0.4.8"
}

variable "region" {
  description = "AWS Region"
  type    = string
  default = "us-west-1"
}

variable "instance_type" {
  description = "AWS EC2 Instance Type"
  type    = string
  default = "t3.medium"
}


variable "public_access" {
  description = "Enable public ingress on port 8000"
  type        = bool
  default     = false // or false depending on your needs
}
