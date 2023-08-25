resource "google_compute_instance" "chroma-db-server-instance-dev" {
  name    = "chroma-db-server-instance-dev"
  project = var.project
  zone    = "europe-north1-c"

  machine_type = "e2-micro"

  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = module.gce-container.source_image
    }
  }

  metadata = {
    gce-container-declaration = module.gce-container.metadata_value
  }


  labels = {
    container-vm = module.gce-container.vm_container_label
  }


  network_interface {
    network = "default"
    access_config {}
  }


  service_account {
    email = "<service_account_with_access_to_artifact_registry>"
    scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]
  }

}

