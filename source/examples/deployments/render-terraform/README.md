# Render.com Deployment

This is an example deployment to Render.com using [terraform](https://www.terraform.io/)

## Requirements

- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)
- [Terraform Render provider](https://registry.terraform.io/providers/jackall3n/render/latest/docs)

## Deployment with terraform

### 1. Init your terraform state

```bash
terraform init
```

### 3. Deploy your application

```bash
# Your Render.com API token. IMPORTANT: The API does not work with Free plan.
export TF_VAR_render_api_token=<render_api_token>
# Your Render.com user email
export TF_VAR_render_user_email=<render_user_email>
#set the chroma release to deploy
export TF_VAR_chroma_release="0.4.13"
# the region to deploy to. At the time of writing only oregon and frankfurt are available
export TF_VAR_region="oregon"
#enable basic auth for the chroma instance
export TF_VAR_enable_auth="true"
#The auth type to use for the chroma instance (token or basic)
export TF_VAR_auth_type="token"
terraform apply -auto-approve
```

### 4. Check your public IP and that Chroma is running

> Note: It might take couple minutes for the instance to boot up

Get the public IP of your instance (it should also be printed out after successful `terraform apply`):

```bash
terraform output instance_url
```

Check that chroma is running:

```bash
export instance_public_ip=$(terraform output instance_url | sed 's/"//g')
curl -v $instance_public_ip/api/v2/heartbeat
```

#### 4.1 Checking Auth

##### Token

When token auth is enabled (this is the default option) you can check the get the credentials from Terraform state by
running:

```bash
terraform output chroma_auth_token
```

You should see something of the form:

```bash
PVcQ4qUUnmahXwUgAf3UuYZoMlos6MnF
```

You can then export these credentials:

```bash
export CHROMA_AUTH=$(terraform output chroma_auth_token | sed 's/"//g')
```

Using the credentials:

```bash
curl -v $instance_public_ip/api/v2/collections -H "Authorization: Bearer ${CHROMA_AUTH}"
```

##### Basic

When basic auth is enabled you can check the get the credentials from Terraform state by running:

```bash
terraform output chroma_auth_basic
```

You should see something of the form:

```bash
chroma:VuA8I}QyNrm0@QLq
```

You can then export these credentials:

```bash
export CHROMA_AUTH=$(terraform output chroma_auth_basic | sed 's/"//g')
```

Using the credentials:

```bash
curl -v https://$instance_public_ip:8000/api/v2/collections -u "${CHROMA_AUTH}"
```

> Note: Without `-u` you should be getting 401 Unauthorized response

#### 4.2 SSH to your instance

To connect to your instance via SSH you need to go to Render.com service dashboard.

### 5. Destroy your application

```bash
terraform destroy
```
