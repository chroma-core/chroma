terraform {
  required_providers {
    render = {
      source  = "jackall3n/render"
      version = "~> 1.3.0"
    }
  }
}

variable "render_api_token" {
  sensitive = true
}

variable "render_user_email" {
  sensitive = true
}

provider "render" {
  api_key = var.render_api_token
}

data "render_owner" "render_owner" {
  email = var.render_user_email
}

resource "render_service" "chroma" {
  name        = "chroma"
  owner       = data.render_owner.render_owner.id
  type        = "web_service"
  auto_deploy = true

  env_vars = concat([{
    key   = "IS_PERSISTENT"
    value = "1"
    },
    {
      key   = "PERSIST_DIRECTORY"
      value = var.chroma_data_volume_mount_path
    },
    ],
    var.enable_auth ? [
      {
        key   = "CHROMA_SERVER_AUTHN_CREDENTIALS"
        value = "${local.token_auth_credentials.token}"
      },
      {
        key   = "CHROMA_SERVER_AUTHN_PROVIDER"
        value = var.auth_type
    }] : []
  )

  image = {
    owner_id   = data.render_owner.render_owner.id
    image_path = "${var.chroma_image_reg_url}:${var.chroma_release}"
  }

  web_service_details = {
    env               = "image"
    plan              = var.render_plan
    region            = var.region
    health_check_path = "/api/v2/heartbeat"
    disk = {
      name       = var.chroma_data_volume_device_name
      mount_path = var.chroma_data_volume_mount_path
      size_gb    = var.chroma_data_volume_size
    }
    docker = {
      command = "uvicorn chromadb.app:app --reload --workers 1 --host 0.0.0.0 --port 80 --log-config chromadb/log_config.yml --timeout-keep-alive 30"
      path    = "./Dockerfile"
    }
  }
}

output "service_id" {
  value = render_service.chroma.id
}

output "instance_url" {
  value = render_service.chroma.web_service_details.url
}

output "chroma_auth_token" {
  value     = random_password.chroma_token.result
  sensitive = true
}
