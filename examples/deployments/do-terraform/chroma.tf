terraform {
  required_providers {
    digitalocean = {
      source = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

# Define provider
variable "do_token" {}

# Configure the DigitalOcean Provider
provider "digitalocean" {
  token = var.do_token
}


resource "digitalocean_firewall" "chroma_firewall" {
  name = "chroma-firewall"

  droplet_ids = [digitalocean_droplet.chroma_instance.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.mgmt_source_ranges
  }

  dynamic "inbound_rule" {
    for_each = var.public_access ? [1] : []
    content {
      protocol         = "tcp"
      port_range       = var.chroma_port
      source_addresses = var.source_ranges
    }
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  tags = local.tags

}

resource "digitalocean_ssh_key" "chroma_keypair" {
  name       = "chroma_keypair"
  public_key = file(var.ssh_public_key)
}


#Create Droplet
resource "digitalocean_droplet" "chroma_instance" {
  image  = var.instance_image
  name   = "chroma"
  region = var.region
  size   = var.instance_type
  ssh_keys = [digitalocean_ssh_key.chroma_keypair.fingerprint]

  user_data = data.template_file.user_data.rendered

  tags = local.tags
}


resource "digitalocean_volume" "chroma_volume" {
  region                  = digitalocean_droplet.chroma_instance.region
  name                    = "chroma-volume"
  size                    = var.chroma_data_volume_size
  description             = "Chroma data volume"
  tags = local.tags
}

resource "digitalocean_volume_attachment" "chroma_data_volume_attachment" {
  droplet_id = digitalocean_droplet.chroma_instance.id
  volume_id  = digitalocean_volume.chroma_volume.id

  provisioner "remote-exec" {
    inline = [
      "export VOLUME_ID=${digitalocean_volume.chroma_volume.name} && sudo mkfs -t ext4 /dev/$(lsblk -o +SERIAL | grep $VOLUME_ID | awk '{print $1}')",
      "sudo mkdir /chroma-data",
      "export VOLUME_ID=${digitalocean_volume.chroma_volume.name} && sudo mount /dev/$(lsblk -o +SERIAL | grep $VOLUME_ID | awk '{print $1}') /chroma-data",
      "cat <<EOF | sudo tee /etc/fstab >> /dev/null",
      "/dev/disk/by-id/scsi-0DO_Volume_${digitalocean_volume.chroma_volume.name} /chroma-data ext4 defaults,nofail,discard 0 0",
      "EOF",
    ]

    connection {
      host = digitalocean_droplet.chroma_instance.ipv4_address
      type = "ssh"
      user = "root"
      private_key = file(var.ssh_private_key)
    }
  }
}


output "instance_public_ip" {
  value = digitalocean_droplet.chroma_instance.ipv4_address
  description = "The public IP address of the Chroma instance"
}

output "instance_private_ip" {
  value = digitalocean_droplet.chroma_instance.ipv4_address_private
  description = "The private IP address of the Chroma instance"
}

output "chroma_auth_token" {
  description = "The Chroma static auth token"
  value = random_password.chroma_token.result
  sensitive = true
}

output "chroma_auth_basic" {
  description = "The Chroma basic auth credentials"
  value = "${local.basic_auth_credentials.username}:${local.basic_auth_credentials.password}"
  sensitive = true
}
