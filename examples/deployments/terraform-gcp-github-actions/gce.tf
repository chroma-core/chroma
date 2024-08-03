resource "google_compute_instance" "chroma-db-server-instance" {
  name    = var.service
  project = var.project
  zone    = var.zone

  machine_type = var.machine_type

  boot_disk {
    initialize_params {
      image = module.gce-container.source_image
    }
  }

  attached_disk {
    source      = google_compute_disk.pd.self_link
    device_name = "data-disk-0"
    mode        = "READ_WRITE"
  }

  metadata = {
    gce-container-declaration    = module.gce-container.metadata_value
    google-logging-enabled       = true
    google-logging-use-fluentbit = true
  }


  labels = {
    container-vm = module.gce-container.vm_container_label
  }


  network_interface {
    network = "default"
    access_config {}
  }


  service_account {
    email = var.service_account
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]
  }

}

resource "google_compute_disk" "pd" {
  project = var.project
  name    = "${var.instance}-disk"
  type    = var.disk_type
  zone    = var.zone
  size    = var.disk_size
}
