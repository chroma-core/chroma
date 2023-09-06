terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.80.0"
    }
  }
}

resource "google_compute_instance" "chroma" {
  project      = var.project_id
  name         = "chroma-1"
  machine_type = var.machine_type
  zone         = var.zone

  tags = local.tags

  labels = var.labels


  boot_disk {
    initialize_params {
      image = var.image
      size  = var.chroma_instance_volume_size #size in GB
    }
  }

  attached_disk {
    source      = google_compute_disk.chroma.id
    device_name = var.chroma_data_volume_device_name
    mode        = "READ_WRITE"
  }

  network_interface {
    network = "default"

    access_config {
      // Ephemeral public IP
    }
  }

  metadata = {
    ssh-keys = "${var.vm_user}:${file(var.ssh_public_key)}"
  }

  metadata_startup_script = templatefile("${path.module}/startup.sh", {
    chroma_release         = var.chroma_release,
    enable_auth            = var.enable_auth,
    auth_type              = var.auth_type,
    basic_auth_credentials = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}",
    token_auth_credentials = random_password.chroma_token.result,
  })

  provisioner "remote-exec" {
    inline = [
      "export VOLUME_ID=${var.chroma_data_volume_device_name} && sudo mkfs -t ext4 /dev/$(lsblk -o +SERIAL | grep $VOLUME_ID | awk '{print $1}')",
      "sudo mkdir /chroma-data",
      "export VOLUME_ID=${var.chroma_data_volume_device_name} && sudo mount /dev/$(lsblk -o +SERIAL | grep $VOLUME_ID | awk '{print $1}') /chroma-data"
    ]

    connection {
      host = google_compute_instance.chroma.network_interface[0].access_config[0].nat_ip
      type = "ssh"
      user = var.vm_user
      private_key = file(var.ssh_private_key)
    }
  }
}


resource "google_compute_disk" "chroma" {
  project = var.project_id
  name    = "chroma-data"
  type    = var.disk_type
  zone    = var.zone
  labels  = var.labels
  size    = var.chroma_data_volume_size #size in GB

  lifecycle {
    prevent_destroy = false #WARNING: You need to configure this manually as the provider does not support it yet
  }
}

#resource "google_compute_attached_disk" "vm_attached_disk" {
#  disk     = google_compute_disk.chroma.id
#  instance = google_compute_instance.chroma.self_link
#
#}



resource "google_compute_firewall" "default" {
  project = var.project_id
  name    = "chroma-firewall"
  network = "default"

  allow {
    protocol = "icmp" #allow ping
  }

  dynamic "allow" {
    for_each = var.public_access ? [1] : []
    content {
      protocol = "tcp"
      ports    = [var.chroma_port]
    }
  }

  source_ranges = var.source_ranges

  target_tags = local.tags
}


output "instance_public_ip" {
  description = "The public IP address of the instance."
  value       = google_compute_instance.chroma.network_interface[0].access_config[0].nat_ip
}

output "chroma_auth_token" {
  value     = random_password.chroma_token.result
  sensitive = true
}


output "chroma_auth_basic" {
  value     = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}"
  sensitive = true
}
