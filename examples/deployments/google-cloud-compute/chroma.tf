resource "google_compute_instance" "chroma1" {
  project      = var.project_id
  name         = "chroma-1"
  machine_type = var.machine_type
  zone         = var.zone

  tags = ["chroma"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 20
    }
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = file("${path.module}/startup.sh")
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
