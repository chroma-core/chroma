# AWS EC2 Basic Deployment

This is an example deployment to AWS EC2 Compute using [terraform](https://www.terraform.io/).

This deployment will do the following:

- Create a security group with required ports open (22 and 8000)
- Create EC2 instance with Ubuntu 22 and deploy Chroma using docker compose
- Create a data volume for Chroma data
- Mount the data volume to the EC2 instance
- Format the data volume with ext4
- Start Chroma

## Requirements

- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)

## Deployment with terraform

This deployment uses Ubuntu 22 as foundation, but you'd like to use a different AMI (non-Debian based image) you may have to adjust the startup script.

To find AWS EC2 AMIs you can use:

```bash
# 099720109477 is Canonical
aws ec2 describe-images \
    --owners 099720109477 \
    --filters 'Name=name,Values=ubuntu/images/*/ubuntu-jammy*' \
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
#AWS access key
export TF_VAR_AWS_ACCESS_KEY=<AWS_ACCESS_KEY>
#AWS secret access key
export TF_VAR_AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
#path to the public key you generated above (or can be different if you want to use your own key)
export TF_ssh_public_key="./chroma-aws.pub"
#path to the private key you generated above (or can be different if you want to use your own key) - used for formatting the Chroma data volume
export TF_ssh_private_key="./chroma-aws"
#set the chroma release to deploy
export TF_VAR_chroma_release=0.4.12
# AWS region to deploy the chroma instance to
export TF_VAR_region="us-west-1"
#enable public access to the chroma instance on port 8000
export TF_VAR_public_access="true"
#enable basic auth for the chroma instance
export TF_VAR_enable_auth="true"
#The auth type to use for the chroma instance (token or basic)
export TF_VAR_auth_type="token"
#optional - if you want to restore from a snapshot
export TF_VAR_chroma_data_restore_from_snapshot_id=""
#optional - if you want to snapshot the data volume before destroying the instance
export TF_VAR_chroma_data_volume_snapshot_before_destroy="true"
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

#### 4.1 Checking Auth

##### Token
When token auth is enabled you can check the get the credentials from Terraform state by running:

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
curl -v http://$instance_public_ip:8000/api/v1/collections -H "Authorization: Bearer ${CHROMA_AUTH}"
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
curl -v http://$instance_public_ip:8000/api/v1/collections -u "${CHROMA_AUTH}"
```

> Note: Without `-u` you should be getting 401 Unauthorized response

#### 4.2 Connect (ssh) to your instance


To SSH to your instance:

```bash
ssh -i ./chroma-aws ubuntu@$instance_public_ip
```

### 5. Destroy your Chroma instance

You will need to change `prevent_destroy` to `false` in the `aws_ebs_volume` in `chroma.tf`.

```bash
terraform destroy -auto-approve
```

## Extras

You can visualize your infrastructure with:

```bash
terraform graph | dot -Tsvg > graph.svg
```

>Note: You will need graphviz installed for this to work
