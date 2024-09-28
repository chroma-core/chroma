# GCP Deployment

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

## A Simple GCP Deployment

You can deploy Chroma on a long-running server, and connect to it
remotely.

For convenience, we have
provided a very simple Terraform configuration to experiment with
deploying Chroma to Google Compute Engine.

{% note type="warning" title="" %}
Chroma and its underlying database [need at least 2GB of RAM](./performance#results-summary),
which means it won't fit on the instances provided as part of the
GCP "always free" tier. This template uses an [`e2-small`](https://cloud.google.com/compute/docs/general-purpose-machines#e2_machine_types) instance, which
costs about two cents an hour, or $15 for a full month, and gives you 2GiB of memory. If you follow these
instructions, GCP will bill you accordingly.
{% /note %}

{% note type="warning" title="" %}
In this guide we show you how to secure your endpoint using [Chroma's
native authentication support](./gcp#authentication-with-gcp). Alternatively, you can put it behind
[GCP API Gateway](https://cloud.google.com/api-gateway/docs) or add your own
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

### Step 1: Set up your GCP credentials

In your GCP project, create a service account for deploying Chroma. It will need the following roles:
* Service Account User
* Compute Admin
* Compute Network Admin
* Storage Admin

Create a JSON key file for this service account, and download it. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your JSON key file:

```shell
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

### Step 2: Install Terraform

Download [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform) and follow the installation instructions for you OS.

### Step 3: Configure your GCP Settings

Create a `chroma.tfvars` file. Use it to define the following variables for your GCP project ID, region, and zone:

```text
project_id="<your project ID>"
region="<your region>"
zone="<your zone>"
```

### Step 4: Initialize and deploy with Terraform

Download our [GCP Terraform configuration](https://github.com/chroma-core/chroma/blob/main/deployments/gcp/main.tf) to the same directory as your `chroma.tfvars` file. Then run the following commands to deploy your Chroma stack.

Initialize Terraform:
```shell
terraform init
```

Plan the deployment, and review it to ensure it matches your expectations:
```shell
terraform plan -var-file chroma.tfvars
```
If you did not customize our configuration, you should be deploying an `e2-small` instance.

Finally, apply the deployment:
```shell
terraform apply -var-file chroma.tfvars
```

#### Customize the Stack (optional)

If  you want to use a machine type different from the default `e2-small`, in your `chroma.tfvars` add the `machine_type` variable and set it to your desired machine:

```text
machine_type = "e2-medium"
```

After a few minutes, you can get the IP address of your instance with
```shell
terraform output -raw chroma_instance_ip
```

### Step 5: Chroma Client Set-Up

Once your Compute Engine instance is up and running with Chroma, all
you need to do is configure your `HttpClient` to use the server's IP address and port
`8000`. Since you are running a Chroma server on GCP, our [thin-client package](./thin-client.md) may be enough for your application.

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

To destroy the stack and remove all GCP resources, use the `terraform destroy` command.

{% note type="warning" title="Note" %}
This will destroy all the data in your Chroma database,
unless you've taken a snapshot or otherwise backed it up.
{% /note %}

```shell
terraform destroy -var-file chroma.tfvars
```

## Authentication with GCP

By default, the Compute Engine instance created by our Terraform configuration will run with no authentication. There are many ways to secure your Chroma instance on GCP. In this guide we will use a simple set-up using Chroma's native authentication support.

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

## Observability with GCP

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