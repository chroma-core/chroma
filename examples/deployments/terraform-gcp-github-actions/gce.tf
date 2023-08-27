resource "google_compute_instance" "chroma-db-server-instance-dev" {
  name    = "chroma-db-server-instance-dev"
  project = var.project
  zone    = "europe-north1-c"

  machine_type = "e2-small"

  allow_stopping_for_update = true

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
  type    = "pd-ssd"
  zone    = var.zone
  size    = 10
}
