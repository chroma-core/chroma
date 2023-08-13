# Google Cloud Compute Deployment

This is an example deployment to Google Cloud Compute using [terraform](https://www.terraform.io/)

## Requirements
- [gcloud CLI](https://cloud.google.com/sdk/gcloud)
- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)

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
```bash
export TF_VAR_project_id=<your_project_id> #take note of this as it must be present in all of the subsequent steps
export TF_VAR_chroma_release=0.4.5 #set the chroma release to deploy
terraform apply -auto-approve
```

### 4. Check your public IP and that Chroma is running

Get the public IP of your instance

```bash
terraform output instance_public_ip
```

Check that chroma is running
```bash
export instance_public_ip=$(terraform output instance_public_ip | sed 's/"//g')
curl -v http://$instance_public_ip:8000/api/v1/heartbeat
```

### 5. Destroy your application
```bash
terraform destroy -auto-approve
```
