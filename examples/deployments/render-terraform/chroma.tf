terraform {
  required_providers {
    render = {
      source = "jackall3n/render"
      version = "~> 1.3.0"
    }
  }
}

variable "render_api_token" {
  sensitive   = true
}

variable "render_user_email" {
  sensitive   = true
}

provider "render" {
  api_key = var.render_api_token
}

data "render_owner" "render_owner" {
  email = var.render_user_email
}

resource "render_service" "chroma" {
  name = "chroma"
  repo = var.chroma_core_repo_url
  branch = "release/${var.chroma_release}"
  owner = data.render_owner.render_owner.id
  type = "web_service"

  web_service_details = {
    env = "python"
    plan = var.render_plan
    region = var.region
    health_check_path = "/api/v1/heartbeat"

    native = {
      build_command = "curl https://raw.githubusercontent.com/amikos-tech/chroma-core/feature/render-terraform-simple/examples/deployments/render-terraform/sqlite_version.patch | git apply && pip install pysqlite3-binary && pip install -r requirements.txt"
      start_command = "uvicorn chromadb.app:app --reload --workers 1 --host 0.0.0.0 --port 80 --log-config chromadb/log_config.yml"
    }

#    disk = {
#      name = var.chroma_data_volume_device_name
#      mount_path = "/chroma-data"
#      size_gb = var.chroma_data_volume_size
#    }
  }
}

resource "render_service_environment" "chroma_env" {
  service = render_service.chroma.id

  variables = [{
      key = "IS_PERSISTENT"
      value = "1"
    },
    {
      key = "PERSIST_DIRECTORY"
      value = "/chroma-data"
    },
    {
      key = "CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER"
      value = "chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"
    },
    {
      key = "CHROMA_SERVER_AUTH_CREDENTIALS"
      value = local.token_auth_credentials.token
    },
    {
      key = "CHROMA_SERVER_AUTH_PROVIDER"
      value = "token"
    }]
}

output "instance_url" {
  value = render_service.chroma.web_service_details.url
}

output "chroma_auth_token" {
  value     = random_password.chroma_token.result
  sensitive = true
}
