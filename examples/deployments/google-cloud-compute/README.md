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
```angular2html
terraform apply -var="project_id=<your_project_id> -auto-approve"
```
