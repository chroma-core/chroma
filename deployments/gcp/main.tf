# Variables
variable "project_id" {
  description = "GCP Project ID"
}

variable "region" {
  description = "GCP Region"
}

variable "zone" {
  description = "GCP Zone"
}

variable "instance_name" {
  description = "Name of the Compute Engine instance"
  default     = "chroma-instance"
}

variable "machine_type" {
  description = "Compute Engine machine type"
  default     = "e2-small"
}

variable "chroma_version" {
  description = "Chroma version to install"
  default     = "0.5.17"
}

variable "chroma_server_auth_credentials" {
  description = "Chroma authentication credentials"
  default     = ""
}

variable "chroma_server_auth_provider" {
  description = "Chroma authentication provider"
  default     = ""
}

variable "chroma_auth_token_transport_header" {
  description = "Chroma authentication custom token header"
  default     = ""
}

variable "chroma_otel_collection_endpoint" {
  description = "Chroma OTEL endpoint"
  default     = ""
}

variable "chroma_otel_service_name" {
  description = "Chroma OTEL service name"
  default     = ""
}

variable "chroma_otel_collection_headers" {
  description = "Chroma OTEL headers"
  default     = "{}"
}

variable "chroma_otel_granularity" {
  description = "Chroma OTEL granularity"
  default     = ""
}

# Provider
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Firewall Rule
resource "google_compute_firewall" "default" {
  name    = "chroma-allow-ssh-http"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22", "8000"]
  }

  source_ranges = ["0.0.0.0/0"]
}

# Compute Engine Instance
resource "google_compute_instance" "chroma_instance" {
  name         = var.instance_name
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 24
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = <<-EOT
  #!/bin/bash
  USER=chroma
  useradd -m -s /bin/bash $USER
  apt-get update
  apt-get install -y docker.io
  usermod -aG docker $USER
  curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
  chmod +x /usr/local/bin/docker-compose
  ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
  systemctl enable docker
  systemctl start docker

  mkdir -p /home/$USER/config
  curl -o /home/$USER/docker-compose.yml https://s3.amazonaws.com/public.trychroma.com/cloudformation/assets/docker-compose.yml
  sed -i "s/CHROMA_VERSION/${var.chroma_version}/g" /home/$USER/docker-compose.yml

  # Create .env file
  echo "CHROMA_SERVER_AUTHN_CREDENTIALS=${var.chroma_server_auth_credentials}" >> /home/$USER/.env
  echo "CHROMA_SERVER_AUTHN_PROVIDER=${var.chroma_server_auth_provider}" >> /home/$USER/.env
  echo "CHROMA_AUTH_TOKEN_TRANSPORT_HEADER=${var.chroma_auth_token_transport_header}" >> /home/$USER/.env
  echo "CHROMA_OTEL_COLLECTION_ENDPOINT=${var.chroma_otel_collection_endpoint}" >> /home/$USER/.env
  echo "CHROMA_OTEL_SERVICE_NAME=${var.chroma_otel_service_name}" >> /home/$USER/.env
  echo "CHROMA_OTEL_COLLECTION_HEADERS=${var.chroma_otel_collection_headers}" >> /home/$USER/.env
  echo "CHROMA_OTEL_GRANULARITY=${var.chroma_otel_granularity}" >> /home/$USER/.env

  chown $USER:$USER /home/$USER/.env /home/$USER/docker-compose.yml
  cd /home/$USER
  sudo -u $USER docker-compose up -d
EOT


  # Tags for firewall rules
  tags = ["chroma-server"]

  # Service account with necessary scopes
  service_account {
    scopes = ["cloud-platform"]
  }
}

# Output
output "chroma_instance_ip" {
  description = "Public IP address of the Chroma server"
  value       = google_compute_instance.chroma_instance.network_interface[0].access_config[0].nat_ip
}
