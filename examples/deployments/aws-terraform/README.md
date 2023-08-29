# AWS EC2 Basic Deployment

This is an example deployment to AWS EC2 Compute using [terraform](https://www.terraform.io/)

## Requirements

- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)

## Deployment with terraform


### 2. Init your terraform state
```bash
terraform init
```

### 3. Deploy your application
```bash
export TF_VAR_AWS_ACCESS_KEY=<AWS_ACCESS_KEY> #take note of this as it must be present in all of the subsequent steps
export TF_VAR_AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> #take note of this as it must be present in all of the subsequent steps
export TF_VAR_chroma_release=0.4.8 #set the chroma release to deploy
export TF_VAR_region="us-west-1" # AWS region to deploy the chroma instance to
export TF_VAR_public_access="false" #enable public access to the chroma instance on port 8000
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

### 5. Destroy your Chroma instance
```bash
terraform destroy -auto-approve
```
