# Azure Deployment

{% Banner type="tip" %}

**Hosted Chroma**

Chroma Cloud, our fully managed hosted service is here. [Sign up here](https://trychroma.com/signup) for early access.

{% /Banner %}

{% Banner type="tip" %}

If you are using Chroma in production, please fill out [this form](https://airtable.com/appqd02UuQXCK5AuY/pagr1D0NFQoNpUpNZ/form), and we will add you to a dedicated Slack workspace for supporting production users.
This is the best place to

1. Get support with building with Chroma in prod.
2. Stay up-to-date with exciting new features.
3. Get swag!

We would love to help you think through the design of your system, or if you would be a good fit for our upcoming distributed cloud service.

{% /Banner %}

## A Simple Azure Deployment

You can deploy Chroma on a long-running server, and connect to it
remotely.

For convenience, we have
provided a very simple Terraform configuration to experiment with
deploying Chroma to Azure.

{% Banner type="warn" %}
Chroma and its underlying database [need at least 2GB of RAM](./performance#results-summary). When defining your VM size for the template in this example, make sure it meets this requirement.
{% /Banner %}

{% Banner type="warn" %}
In this guide we show you how to secure your endpoint using [Chroma's
native authentication support](./azure#authentication-with-azure). Alternatively, you can put it behind
an API Gateway or add your own
authenticating proxy. This basic stack doesn't support any kind of authentication;
anyone who knows your server IP will be able to add and query for
embeddings.
{% /Banner %}

{% Banner type="warn" %}
By default, this template saves all data on a single
volume. When you delete or replace it, the data will disappear. For
serious production use (with high availability, backups, etc.) please
read and understand the Terraform template and use it as a basis
for what you need, or reach out to the Chroma team for assistance.
{% /Banner %}

### Step 1: Install Terraform

Download [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform) and follow the installation instructions for you OS.

### Step 2: Authenticate with Azure

```terminal
az login
```

### Step 3: Configure your Azure Settings

Create a `chroma.tfvars` file. Use it to define the following variables for your Azure Resource Group name, VM size, and location. Note that this template creates a new resource group for your Chroma deployment.

```text
resource_group_name = "your-azure-resource-group-name"
location            = "your-location"
machine_type        = "Standard_B1s"
```

### Step 4: Initialize and deploy with Terraform

Download our [Azure Terraform configuration](https://github.com/chroma-core/chroma/blob/main/deployments/azure/main.tf) to the same directory as your `chroma.tfvars` file. Then run the following commands to deploy your Chroma stack.

Initialize Terraform:
```terminal
terraform init
```

Plan the deployment, and review it to ensure it matches your expectations:
```terminal
terraform plan -var-file chroma.tfvars
```

Finally, apply the deployment:
```terminal
terraform apply -var-file chroma.tfvars
```

After a few minutes, you can get the IP address of your instance with
```terminal
terraform output -raw public_ip_address
```

### Step 5: Chroma Client Set-Up

Once your Azure VM instance is up and running with Chroma, all
you need to do is configure your `HttpClient` to use the server's IP address and port
`8000`. Since you are running a Chroma server on Azure, our [thin-client package](/production/chroma-server/python-thin-client) may be enough for your application.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
import chromadb

chroma_client = chromadb.HttpClient(
    host="<Your Chroma instance IP>",
    port=8000
)
chroma_client.heartbeat()
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({
    path: "<Your Chroma instance IP>",
    port: 8000
})
chromaClient.heartbeat()
```

{% /Tab %}

{% /TabbedCodeBlock %}

### Step 5: Clean Up (optional).

To destroy the stack and remove all Azure resources, use the `terraform destroy` command.

```shell
terraform destroy -var-file chroma.tfvars
```

{% Banner type="warn" %}
This will destroy all the data in your Chroma database,
unless you've taken a snapshot or otherwise backed it up.
{% /Banner %}

## Observability with Azure

Chroma is instrumented with [OpenTelemetry](https://opentelemetry.io/) hooks for observability. We currently only exports OpenTelemetry [traces](https://opentelemetry.io/docs/concepts/signals/traces/). These should allow you to understand how requests flow through the system and quickly identify bottlenecks. Check out the [observability docs](../administration/observability) for a full explanation of the available parameters.

To enable tracing on your Chroma server, simply define the following variables in your `chroma.tfvars`:

```text
chroma_otel_collection_endpoint          = "api.honeycomb.com"
chroma_otel_service_name                 = "chromadb"
chroma_otel_collection_headers           = "{'x-honeycomb-team': 'abc'}"
```
