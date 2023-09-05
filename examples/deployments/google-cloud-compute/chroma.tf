resource "google_compute_instance" "chroma1" {
  project      = var.project_id
  name         = "chroma-1"
  machine_type = var.machine_type
  zone         = var.zone

  tags = ["chroma"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = var.chroma_instance_volume_size #size in GB
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = templatefile("${path.module}/startup.sh", { chroma_release = var.chroma_release })
}

resource "google_compute_disk" "chroma" {
  name  = "test-disk"
  type  = "pd-ssd"
  zone  = "us-central1-a"
  image = "debian-11-bullseye-v20220719"
  labels = {
    environment = "dev"
  }
  physical_block_size_bytes = 4096
}

resource "google_compute_attached_disk" "vm_attached_disk" {
  disk     = google_compute_disk.chroma.id
  instance = google_compute_instance.chroma1.self_link
}

resource "google_compute_firewall" "default" {
  project = var.project_id
  name    = "chroma-firewall"
  network = "default"

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["8000"]
  }

  source_ranges = ["0.0.0.0/0"]

  target_tags = ["chroma"]
}


output "instance_public_ip" {
  description = "The public IP address of the instance."
  value       = google_compute_instance.chroma1.network_interface[0].access_config[0].nat_ip
}

output "chroma_auth_token" {
  value = random_password.chroma_token.result
  sensitive = true
}


output "chroma_auth_basic" {
  value = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}"
  sensitive = true
}
