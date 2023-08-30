# AWS EC2 Basic Deployment

This is an example deployment to AWS EC2 Compute using [terraform](https://www.terraform.io/)

## Requirements

- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)

## Deployment with terraform

This deployment uses Ubuntu 22 as foundation, but you'd like to use a different AMI (non-Debian based image) you may have to adjust the startup script.

To find AWS EC2 AMIs you can use:

```bash
# 099720109477 is Canonical
aws ec2 describe-images \
    --owners 099720109477 \
    --filters 'Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy*' \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId'
```

### 2. Init your terraform state
```bash
terraform init
```

### 3. Deploy your application

Generate SSH key to use with your chroma instance (so you can login to the EC2):

> Note: This is optional. You can use your own existing SSH key if you prefer.

```bash
ssh-keygen -t RSA -b 4096 -C "Chroma AWS Key" -N "" -f ./chroma-aws && chmod 400 ./chroma-aws
```

Set up your Terraform variables and deploy your instance:

```bash
export TF_VAR_AWS_ACCESS_KEY=<AWS_ACCESS_KEY> #take note of this as it must be present in all of the subsequent steps
export TF_VAR_AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY> #take note of this as it must be present in all of the subsequent steps
export TF_ssh_public_key="./chroma-aws.pub" #path to the public key you generated above (or can be different if you want to use your own key)
export TF_VAR_chroma_release=0.4.8 #set the chroma release to deploy
export TF_VAR_region="us-west-1" # AWS region to deploy the chroma instance to
export TF_VAR_public_access="false" #enable public access to the chroma instance on port 8000
export TF_VAR_enable_auth="true" #enable basic auth for the chroma instance
export TF_VAR_basic_auth_credentials="admin:Chr0m4%!" #basic credentials for the chroma instance
terraform apply -auto-approve
```
> Note: Basic Auth is supported by Chroma v0.4.7+

### 4. Check your public IP and that Chroma is running

Get the public IP of your instance

```bash
terraform output instance_public_ip
```

Check that chroma is running (It should take up several minutes for the instance to be ready)

```bash
export instance_public_ip=$(terraform output instance_public_ip | sed 's/"//g')
curl -v http://$instance_public_ip:8000/api/v1/heartbeat
```

When auth is enabled you will need to pass the basic auth credentials (`-u`):

```bash
curl -v http://$instance_public_ip:8000/api/v1/collections -u "admin:Chr0m4%!"
```

> Note: Without `-u` you should be get 401 Unauthorized

To SSH to your instance:

```bash
ssh -i ./chroma-aws ubuntu@$instance_public_ip
```

### 5. Destroy your Chroma instance
```bash
terraform destroy -auto-approve
```
