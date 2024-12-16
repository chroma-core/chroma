---
title: "☁️ Azure Deployment"
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

{% note type="tip" title="Hosted Chroma" %}
Chroma Cloud, our fully managed hosted service, is in early access. Fill out the survey to jump the waitlist and get the best retrieval experience. Full access coming Q1 2025.

[📝 30 second survey](https://airtable.com/shrOAiDUtS2ILy5vZ)

{% /note %}

{% note type="tip" title="" %}
If you are using Chroma in production, please fill out [this form](https://airtable.com/appqd02UuQXCK5AuY/pagr1D0NFQoNpUpNZ/form), and we will add you to a dedicated Slack workspace for supporting production users.
This is the best place to

1. Get support with building with Chroma in prod.
2. Stay up-to-date with exciting new features.
3. Get swag!

We would love to help you think through the design of your system, or if you would be a good fit for our upcoming distributed cloud service.
{% /note %}

## A Simple Azure Deployment

You can deploy Chroma on a long-running server, and connect to it
remotely.

For convenience, we have
provided a very simple Terraform configuration to experiment with
deploying Chroma to Azure.

{% note type="warning" title="" %}
Chroma and its underlying database [need at least 2GB of RAM](./performance#results-summary). When defining your VM size for the template in this example, make sure it meets this requirement.
{% /note %}

{% note type="warning" title="" %}
In this guide we show you how to secure your endpoint using [Chroma's
native authentication support](./azure#authentication-with-azure). Alternatively, you can put it behind
an API Gateway or add your own
authenticating proxy. This basic stack doesn't support any kind of authentication;
anyone who knows your server IP will be able to add and query for
embeddings.
{% /note %}

{% note type="warning" title="" %}
By default, this template saves all data on a single
volume. When you delete or replace it, the data will disappear. For
serious production use (with high availability, backups, etc.) please
read and understand the Terraform template and use it as a basis
for what you need, or reach out to the Chroma team for assistance.
{% /note %}

### Step 1: Install Terraform

Download [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform) and follow the installation instructions for you OS.

### Step 2: Authenticate with Azure

```shell
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
```shell
terraform init
```

Plan the deployment, and review it to ensure it matches your expectations:
```shell
terraform plan -var-file chroma.tfvars
```

Finally, apply the deployment:
```shell
terraform apply -var-file chroma.tfvars
```

After a few minutes, you can get the IP address of your instance with
```shell
terraform output -raw public_ip_address
```

### Step 5: Chroma Client Set-Up

Once your Azure VM instance is up and running with Chroma, all
you need to do is configure your `HttpClient` to use the server's IP address and port
`8000`. Since you are running a Chroma server on Azure, our [thin-client package](./thin-client.md) may be enough for your application.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
import chromadb

chroma_client = chromadb.HttpClient(
    host="<Your Chroma instance IP>",
    port=8000
)
chroma_client.heartbeat()
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({
    path: "<Your Chroma instance IP>",
    port: 8000
})
chromaClient.heartbeat()
```

{% /tab %}
{% /tabs %}

### Step 5: Clean Up (optional).

To destroy the stack and remove all Azure resources, use the `terraform destroy` command.

{% note type="warning" title="Note" %}
This will destroy all the data in your Chroma database,
unless you've taken a snapshot or otherwise backed it up.
{% /note %}

```shell
terraform destroy -var-file chroma.tfvars
```

## Authentication with Azure

By default, the Azure VM instance created by our Terraform configuration will run with no authentication. There are many ways to secure your Chroma instance on Azure. In this guide we will use a simple set-up using Chroma's native authentication support.

You can learn more about authentication with Chroma in the [Auth Guide](/deployment/auth).

### Static API Token Authentication

#### Customize Chroma's Terraform Configuration

{% note type="note" title="Security Note" %}
Current implementation of static API token auth supports only ENV based tokens. Tokens must be alphanumeric ASCII strings. Tokens are case-sensitive.
{% /note %}

If, for example, you want the static API token to be "test-token", set the following variables in your `chroma.tfvars`. This will set `Authorization: Bearer test-token` as your authentication header.

```text
chroma_server_auth_credentials           = "test-token"
chroma_server_auth_provider              = "chromadb.auth.token_authn.TokenAuthenticationServerProvider"
```

To use `X-Chroma-Token: test-token` type of authentication header you can set the `ChromaAuthTokenTransportHeader` parameter:

```text
chroma_server_auth_credentials           = "test-token"
chroma_server_auth_provider              = "chromadb.auth.token_authn.TokenAuthenticationServerProvider"
chroma_auth_token_transport_header       = "X-Chroma-Token"
```

#### Client Set-Up

Add the `CHROMA_CLIENT_AUTH_CREDENTIALS` environment variable to your local environment, and set it to the token you provided the server (`test-token` in this example):

```shell
export CHROMA_CLIENT_AUTH_CREDENTIALS="test-token"
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

We will use Chroma's `Settings` object to define the authentication method on the client.

```python
import os
import chromadb
from chromadb.config import Settings
from dotenv import load_dotenv

load_dotenv()

client = chromadb.HttpClient(
    host="<Your Chroma Instance IP>",
    port=8000,
    settings=Settings(
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials=os.getenv("CHROMA_CLIENT_AUTH_CREDENTIALS")
    )
)

client.heartbeat()
```

If you are using a custom `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` (like `X-Chroma-Token`), add it to your `Settings`:

```python
chroma_auth_token_transport_header=os.getenv("CHROMA_AUTH_TOKEN_TRANSPORT_HEADER")
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({
    path: "<Your Chroma Instance IP>",
    auth: {
        provider: "token",
        credentials: process.env.CHROMA_CLIENT_AUTH_CREDENTIALS,
        tokenHeaderType: process.env.CHROMA_AUTH_TOKEN_TRANSPORT_HEADER
    }
})

chromaClient.heartbeat()
```

{% /tab %}
{% /tabs %}

## Observability with Azure

Chroma is instrumented with [OpenTelemetry](https://opentelemetry.io/) hooks for observability. We currently only exports OpenTelemetry [traces](https://opentelemetry.io/docs/concepts/signals/traces/). These should allow you to understand how requests flow through the system and quickly identify bottlenecks.

Tracing is configured with four environment variables:

- `CHROMA_OTEL_COLLECTION_ENDPOINT`: where to send observability data. Example: `api.honeycomb.com`.
- `CHROMA_OTEL_SERVICE_NAME`: Service name for OTel traces. Default: `chromadb`.
- `CHROMA_OTEL_COLLECTION_HEADERS`: Headers to use when sending observability data. Often used to send API and app keys. For example `{"x-honeycomb-team": "abc"}`.
- `CHROMA_OTEL_GRANULARITY`: A value from the [OpenTelemetryGranularity enum](https://github.com/chroma-core/chroma/tree/main/chromadb/telemetry/opentelemetry/__init__.py). Specifies how detailed tracing should be.

To enable tracing on your Chroma server, simply define the following variables in your `chroma.tfvars`:

```text
chroma_otel_collection_endpoint          = "api.honeycomb.com"
chroma_otel_service_name                 = "chromadb"
chroma_otel_collection_headers           = "{'x-honeycomb-team': 'abc'}"
chroma_otel_granularity                  = "all"
```