---
id: gcp
name: GCP
---

# GCP Deployment

{% Banner type="tip" %}

**Chroma Cloud**

Chroma Cloud, our fully managed hosted service is here. [Sign up here](https://trychroma.com/signup?utm_source=docs-gcp) for free.

{% /Banner %}

## A Simple GCP Deployment

You can deploy Chroma on a long-running server, and connect to it
remotely.

For convenience, we have
provided a very simple Terraform configuration to experiment with
deploying Chroma to Google Compute Engine.

{% Banner type="warn" %}

Chroma and its underlying database [need at least 2GB of RAM](./performance#results-summary),
which means it won't fit on the instances provided as part of the
GCP "always free" tier. This template uses an [`e2-small`](https://cloud.google.com/compute/docs/general-purpose-machines#e2_machine_types) instance, which
costs about two cents an hour, or $15 for a full month, and gives you 2GiB of memory. If you follow these
instructions, GCP will bill you accordingly.

{% /Banner %}

{% Banner type="warn" %}

In this guide we show you how to secure your endpoint using [Chroma's
native authentication support](./gcp#authentication-with-gcp). Alternatively, you can put it behind
[GCP API Gateway](https://cloud.google.com/api-gateway/docs) or add your own
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

### Step 1: Set up your GCP credentials

In your GCP project, create a service account for deploying Chroma. It will need the following roles:

- Service Account User
- Compute Admin
- Compute Network Admin
- Storage Admin

Create a JSON key file for this service account, and download it. Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of your JSON key file:

```terminal
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

### Step 2: Install Terraform

Download [Terraform](https://developer.hashicorp.com/terraform/install?product_intent=terraform) and follow the installation instructions for your OS.

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

```terminal
terraform init
```

Plan the deployment, and review it to ensure it matches your expectations:

```terminal
terraform plan -var-file chroma.tfvars
```

If you did not customize our configuration, you should be deploying an `e2-small` instance.

Finally, apply the deployment:

```terminal
terraform apply -var-file chroma.tfvars
```

#### Customize the Stack (optional)

If you want to use a machine type different from the default `e2-small`, in your `chroma.tfvars` add the `machine_type` variable and set it to your desired machine:

```text
machine_type = "e2-medium"
```

After a few minutes, you can get the IP address of your instance with

```terminal
terraform output -raw chroma_instance_ip
```

### Step 5: Chroma Client Set-Up

{% Tabs %}

{% Tab label="python" %}
Once your Compute Engine instance is up and running with Chroma, all
you need to do is configure your `HttpClient` to use the server's IP address and port
`8000`. Since you are running a Chroma server on Azure, our [thin-client package](./python-thin-client) may be enough for your application.

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
Once your Compute Engine instance is up and running with Chroma, all
you need to do is configure your `ChromaClient` to use the server's IP address and port
`8000`.

```typescript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({
  host: "<Your Chroma instance IP>",
  port: 8000,
});
chromaClient.heartbeat();
```

{% /Tab %}

{% /Tabs %}

### Step 5: Clean Up (optional).

To destroy the stack and remove all GCP resources, use the `terraform destroy` command.

{% note type="warning" title="Note" %}
This will destroy all the data in your Chroma database,
unless you've taken a snapshot or otherwise backed it up.
{% /note %}

```terminal
terraform destroy -var-file chroma.tfvars
```

## Observability with GCP

Chroma is instrumented with [OpenTelemetry](https://opentelemetry.io/) hooks for observability. We currently only exports OpenTelemetry [traces](https://opentelemetry.io/docs/concepts/signals/traces/). These should allow you to understand how requests flow through the system and quickly identify bottlenecks. Check out the [observability docs](../administration/observability) for a full explanation of the available parameters.

To enable tracing on your Chroma server, simply define the following variables in your `chroma.tfvars`:

```text
chroma_otel_collection_endpoint          = "api.honeycomb.com"
chroma_otel_service_name                 = "chromadb"
chroma_otel_collection_headers           = "{'x-honeycomb-team': 'abc'}"
```
