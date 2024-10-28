variable "resource_group_name" {
  description = "Name of the Azure Resource Group"
}

variable "location" {
  description = "Azure region where resources will be deployed"
}

variable "instance_name" {
  description = "Name of the Virtual Machine"
  default     = "chroma-instance"
}

variable "machine_type" {
  description = "Azure VM size (e.g., Standard_B1s)"
}

variable "chroma_version" {
  description = "Chroma version to install"
  default     = "0.5.16"
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

variable "ssh_public_key_path" {
  description = "Path to your SSH public key"
  default     = "~/.ssh/id_rsa.pub"
}

provider "azurerm" {
  features = {}
}

# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

# Virtual Network
resource "azurerm_virtual_network" "vnet" {
  name                = "${var.instance_name}-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
}

# Subnet
resource "azurerm_subnet" "subnet" {
  name                 = "${var.instance_name}-subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Network Security Group
resource "azurerm_network_security_group" "nsg" {
  name                = "${var.instance_name}-nsg"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name

  security_rule {
    name                       = "AllowSSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "AllowHTTP"
    priority                   = 1002
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8000"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

# Public IP
resource "azurerm_public_ip" "public_ip" {
  name                = "${var.instance_name}-publicip"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
}

# Network Interface
resource "azurerm_network_interface" "nic" {
  name                = "${var.instance_name}-nic"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "ipconfig1"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.public_ip.id
  }
}

# Associate NSG with NIC
resource "azurerm_network_interface_security_group_association" "nsg_association" {
  network_interface_id      = azurerm_network_interface.nic.id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

# Virtual Machine
resource "azurerm_linux_virtual_machine" "vm" {
  name                = var.instance_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  size                = var.machine_type
  admin_username      = "azureuser"

  network_interface_ids = [
    azurerm_network_interface.nic.id,
  ]

  admin_ssh_key {
    username   = "azureuser"
    public_key = file(var.ssh_public_key_path)
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
    disk_size_gb         = 24
  }

  source_image_reference {
    publisher = "Debian"
    offer     = "debian-11"
    sku       = "11"
    version   = "latest"
  }

  custom_data = base64encode(<<-EOT
    #!/bin/bash
    USER=chroma
    useradd -m -s /bin/bash $USER
    apt-get update
    apt-get install -y docker.io
    usermod -aG docker $USER
    curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-\$(uname -s)-\$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose
    systemctl enable docker
    systemctl start docker

    mkdir -p /home/\$USER/config
    curl -o /home/\$USER/docker-compose.yml https://s3.amazonaws.com/public.trychroma.com/cloudformation/assets/docker-compose.yml
    sed -i "s/CHROMA_VERSION/${var.chroma_version}/g" /home/\$USER/docker-compose.yml

    # Create .env file
    echo "CHROMA_SERVER_AUTHN_CREDENTIALS=${var.chroma_server_auth_credentials}" >> /home/\$USER/.env
    echo "CHROMA_SERVER_AUTHN_PROVIDER=${var.chroma_server_auth_provider}" >> /home/\$USER/.env
    echo "CHROMA_AUTH_TOKEN_TRANSPORT_HEADER=${var.chroma_auth_token_transport_header}" >> /home/\$USER/.env
    echo "CHROMA_OTEL_COLLECTION_ENDPOINT=${var.chroma_otel_collection_endpoint}" >> /home/\$USER/.env
    echo "CHROMA_OTEL_SERVICE_NAME=${var.chroma_otel_service_name}" >> /home/\$USER/.env
    echo "CHROMA_OTEL_COLLECTION_HEADERS=${var.chroma_otel_collection_headers}" >> /home/\$USER/.env
    echo "CHROMA_OTEL_GRANULARITY=${var.chroma_otel_granularity}" >> /home/\$USER/.env

    chown \$USER:\$USER /home/\$USER/.env /home/\$USER/docker-compose.yml
    cd /home/\$USER
    sudo -u \$USER docker-compose up -d
  EOT
  )
}

# Output
output "public_ip_address" {
  description = "Public IP address of the Chroma server"
  value       = azurerm_public_ip.public_ip.ip_address
}
