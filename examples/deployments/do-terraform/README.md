# Digital Ocean Droplet Deployment

This is an example deployment using Digital Ocean Droplet using [terraform](https://www.terraform.io/).

This deployment will do the following:

- 🔥 Create a firewall with required ports open (22 and 8000)
- 🐳 Create Droplet with Ubuntu 22 and deploy Chroma using docker compose
- 💿 Create a data volume for Chroma data
- 🗻 Mount the data volume to the Droplet instance
- ✏️ Format the data volume with ext4
- 🏃‍ Start Chroma

## Requirements

- [Terraform CLI v1.3.4+](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli)

## Deployment with terraform

This deployment uses Ubuntu 22 as foundation, but you'd like to use a different image for your Droplet (
see  https://slugs.do-api.dev/ for a list of available images)

### Configuration Options


### 1. Init your terraform state

```bash
terraform init
```

### 2. Deploy your application

Generate SSH key to use with your chroma instance (so you can log in to the Droplet):

> Note: This is optional. You can use your own existing SSH key if you prefer.

```bash
ssh-keygen -t RSA -b 4096 -C "Chroma DO Key" -N "" -f ./chroma-do && chmod 400 ./chroma-do
```

Set up your Terraform variables and deploy your instance:

```bash
#take note of this as it must be present in all of the subsequent steps
export TF_VAR_do_token=<DIGITALOCEAN_TOKEN>
#path to the public key you generated above (or can be different if you want to use your own key)
export TF_ssh_public_key="./chroma-do.pub"
#path to the private key you generated above (or can be different if you want to use your own key) - used for formatting the Chroma data volume
export TF_ssh_private_key="./chroma-do"
#set the chroma release to deploy
export TF_VAR_chroma_release="0.4.12"
# DO region to deploy the chroma instance to
export TF_VAR_region="ams2"
#enable public access to the chroma instance on port 8000
export TF_VAR_public_access="true"
#enable basic auth for the chroma instance
export TF_VAR_enable_auth="true"
#The auth type to use for the chroma instance (token or basic)
export TF_VAR_auth_type="token"
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
curl -v http://$instance_public_ip:8000/api/v2/heartbeat
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
ssh -i ./chroma-do root@$instance_public_ip
```

### 5. Destroy your Chroma instance

```bash
terraform destroy -auto-approve
```

## Extras

You can visualize your infrastructure with:

```bash
terraform graph | dot -Tsvg > graph.svg
```

> Note: You will need graphviz installed for this to work

### Digital Ocean Resource Types

Refs: https://slugs.do-api.dev/
