# Google Cloud Compute Deployment

This is an example deployment to Google Cloud Compute using [terraform](https://www.terraform.io/)

## Requirements

- [gcloud CLI](https://cloud.google.com/sdk/gcloud)
- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)
- [Terraform GCP provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

## Deployment with terraform

### 1. Auth to your Google Cloud project

```bash
gcloud auth application-default login
```

### 2. Init your terraform state

```bash
terraform init
```

### 3. Deploy your application

> **WARNING**: GCP Terraform provider does not allow use of variables in the lifecycle of the volume. By default, the
> template does not prevent deletion of the volume however if you plan to use this template for production deployment you
> may consider change the value of `prevent_destroy` to `true` in `chroma.tf` file.

Generate SSH key to use with your chroma instance (so you can SSH to the GCP VM):

> Note: This is optional. You can use your own existing SSH key if you prefer.

```bash
ssh-keygen -t RSA -b 4096 -C "Chroma AWS Key" -N "" -f ./chroma-aws && chmod 400 ./chroma-aws
```

```bash
export TF_VAR_project_id=<your_project_id> #take note of this as it must be present in all of the subsequent steps
export TF_ssh_public_key="./chroma-aws.pub" #path to the public key you generated above (or can be different if you want to use your own key)
export TF_ssh_private_key="./chroma-aws" #path to the private key you generated above (or can be different if you want to use your own key) - used for formatting the Chroma data volume
export TF_VAR_chroma_release="0.4.9" #set the chroma release to deploy
export TF_VAR_zone="us-central1-a" # AWS region to deploy the chroma instance to
export TF_VAR_public_access="true" #enable public access to the chroma instance on port 8000
export TF_VAR_enable_auth="true" #enable basic auth for the chroma instance
export TF_VAR_auth_type="token" #The auth type to use for the chroma instance (token or basic)
terraform apply -auto-approve
```

### 4. Check your public IP and that Chroma is running

> Note: Depending on your instance type it might take a few minutes for the instance to be ready

Get the public IP of your instance (it should also be printed out after successful `terraform apply`):

```bash
terraform output instance_public_ip
```

Check that chroma is running:

```bash
export instance_public_ip=$(terraform output instance_public_ip | sed 's/"//g')
curl -v http://$instance_public_ip:8000/api/v2/heartbeat
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
curl -v http://$instance_public_ip:8000/api/v2/collections -H "Authorization: Bearer ${CHROMA_AUTH}"
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
curl -v http://$instance_public_ip:8000/api/v2/collections -u "${CHROMA_AUTH}"
```

> Note: Without `-u` you should be getting 401 Unauthorized response

#### 4.2 SSH to your instance

To SSH to your instance:

```bash
ssh -i ./chroma-aws debian@$instance_public_ip
```

### 5. Destroy your application

```bash
terraform destroy -auto-approve
```
