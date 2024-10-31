resource_group_name = "your-azure-resource-group-name"
location            = "your-location"
machine_type        = "Standard_B1s"            # Azure VM size
ssh_public_key_path = "~/.ssh/id_rsa.pub"       # Path to your SSH public key

instance_name                           = "chroma-instance"
chroma_version                          = "0.5.17"
chroma_server_auth_credentials          = ""
chroma_server_auth_provider             = ""
chroma_auth_token_transport_header      = ""
chroma_otel_collection_endpoint         = ""
chroma_otel_service_name                = ""
chroma_otel_collection_headers          = "{}"
chroma_otel_granularity                 = ""
