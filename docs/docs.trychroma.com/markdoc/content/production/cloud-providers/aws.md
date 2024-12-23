# AWS Deployment

{% Banner type="tip" %}

**Hosted Chroma**

Chroma Cloud, our fully managed hosted service, is in early access. Fill out the survey to jump the waitlist and get the best retrieval experience. Full access coming Q1 2025.

[📝 30 second survey](https://airtable.com/shrOAiDUtS2ILy5vZ)

{% /Banner %}

{% Banner type="tip" %}

If you are using Chroma in production, please fill out [this form](https://airtable.com/appqd02UuQXCK5AuY/pagr1D0NFQoNpUpNZ/form), and we will add you to a dedicated Slack workspace for supporting production users.
This is the best place to

1. Get support with building with Chroma in prod.
2. Stay up-to-date with exciting new features.
3. Get swag!

We would love to help you think through the design of your system, or if you would be a good fit for our upcoming distributed cloud service.

{% /Banner %}

## A Simple AWS Deployment

You can deploy Chroma on a long-running server, and connect to it
remotely.

There are many possible configurations, but for convenience we have
provided a very simple AWS CloudFormation template to experiment with
deploying Chroma to EC2 on AWS.

{% Banner type="warn" %}

Chroma and its underlying database [need at least 2GB of RAM](./performance#results-summary),
which means it won't fit on the 1gb instances provided as part of the
AWS Free Tier. This template uses a [`t3.small`](https://aws.amazon.com/ec2/instance-types/t3/#Product%20Details) EC2 instance, which
costs about two cents an hour, or $15 for a full month, and gives you 2GiB of memory. If you follow these
instructions, AWS will bill you accordingly.

{% /Banner %}

{% Banner type="warn" %}

In this guide we show you how to secure your endpoint using [Chroma's
native authentication support](./aws#authentication-with-aws). Alternatively, you can put it behind
[AWS API Gateway](https://aws.amazon.com/api-gateway/) or add your own
authenticating proxy. This basic stack doesn't support any kind of authentication;
anyone who knows your server IP will be able to add and query for
embeddings.

{% /Banner %}

{% Banner type="warn"  %}

By default, this template saves all data on a single
volume. When you delete or replace it, the data will disappear. For
serious production use (with high availability, backups, etc.) please
read and understand the CloudFormation template and use it as a basis
for what you need, or reach out to the Chroma team for assistance.

{% /Banner %}

### Step 1: Get an AWS Account

You will need an AWS Account. You can use one you already have, or
[create a new one](https://aws.amazon.com).

### Step 2: Get credentials

For this example, we will be using the AWS command line
interface. There are
[several ways](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)
to configure the AWS CLI, but for the purposes of these examples we
will presume that you have
[obtained an AWS access key](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html)
and will be using environment variables to configure AWS.

Export the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables in your shell:

```terminal
export AWS_ACCESS_KEY_ID=**\*\***\*\*\*\***\*\***
export AWS_SECRET_ACCESS_KEY=****\*\*****\*\*****\*\*****
```

You can also configure AWS to use a region of your choice using the
`AWS_REGION` environment variable:

```terminal
export AWS_REGION=us-east-1
```

### Step 3: Run CloudFormation

Chroma publishes a [CloudFormation template](https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/chroma.cf.json) to S3 for each release.

To launch the template using AWS CloudFormation, run the following command line invocation.

Replace `--stack-name my-chroma-stack` with a different stack name, if you wish.

```terminal
aws cloudformation create-stack --stack-name my-chroma-stack --template-url https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/chroma.cf.json
```

Wait a few minutes for the server to boot up, and Chroma will be
available! You can get the public IP address of your new Chroma server using the AWS console, or using the following command:

```terminal
aws cloudformation describe-stacks --stack-name my-chroma-stack --query 'Stacks[0].Outputs'
```

Note that even after the IP address of your instance is available, it may still take a few minutes for Chroma to be up and running.

#### Customize the Stack (optional)

The CloudFormation template allows you to pass particular key/value
pairs to override aspects of the stack. Available keys are:

- `InstanceType` - the AWS instance type to run (default: `t3.small`)
- `KeyName` - the AWS EC2 KeyPair to use, allowing to access the instance via SSH (default: none)

To set a CloudFormation stack's parameters using the AWS CLI, use the
`--parameters` command line option. Parameters must be specified using
the format `ParameterName={parameter},ParameterValue={value}`.

For example, the following command launches a new stack similar to the
above, but on a `m5.4xlarge` EC2 instance, and adding a KeyPair named
`mykey` so anyone with the associated private key can SSH into the
machine:

```terminal
aws cloudformation create-stack --stack-name my-chroma-stack --template-url https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/chroma.cf.json \
 --parameters ParameterKey=KeyName,ParameterValue=mykey \
 ParameterKey=InstanceType,ParameterValue=m5.4xlarge
```

### Step 4: Chroma Client Set-Up

Once your EC2 instance is up and running with Chroma, all
you need to do is configure your `HttpClient` to use the server's IP address and port
`8000`. Since you are running a Chroma server on AWS, our [thin-client package](../chroma-server/python-thin-client) may be enough for your application.

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

To destroy the stack and remove all AWS resources, use the AWS CLI `delete-stack` command.

{% note type="warning" title="Note" %}
This will destroy all the data in your Chroma database,
unless you've taken a snapshot or otherwise backed it up.
{% /note %}

```terminal
aws cloudformation delete-stack --stack-name my-chroma-stack
```

## Authentication with AWS

By default, the EC2 instance created by our CloudFormation template will run with no authentication. There are many ways to secure your Chroma instance on AWS. In this guide we will use a simple set-up using Chroma's native authentication support.

You can learn more about authentication with Chroma in the [Auth Guide](../administration/auth).

### Static API Token Authentication

#### Customize Chroma's CloudFormation Stack

{% Banner type="note" %}

**Security Note**

Current implementation of static API token auth supports only ENV based tokens. Tokens must be alphanumeric ASCII strings. Tokens are case-sensitive.

{% /Banner %}

If, for example, you want the static API token to be "test-token", pass the following parameters when creating your Chroma stack. This will set `Authorization: Bearer test-token` as your authentication header.

```terminal
aws cloudformation create-stack --stack-name my-chroma-stack --template-url https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/chroma.cf.json \
 --parameters ParameterKey=ChromaServerAuthCredentials,ParameterValue="test-token" \
 ParameterKey=ChromaServerAuthProvider,ParameterValue="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
```

To use `X-Chroma-Token: test-token` type of authentication header you can set the `ChromaAuthTokenTransportHeader` parameter:

```terminal
aws cloudformation create-stack --stack-name my-chroma-stack --template-url https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/chroma.cf.json \
 --parameters ParameterKey=ChromaServerAuthCredentials,ParameterValue="test-token" \
 ParameterKey=ChromaServerAuthProvider,ParameterValue="chromadb.auth.token_authn.TokenAuthenticationServerProvider" \
 ParameterKey=ChromaAuthTokenTransportHeader,ParameterValue="X-Chroma-Token"
```

#### Client Set-Up

Add the `CHROMA_CLIENT_AUTH_CREDENTIALS` environment variable to your local environment, and set it to the token you provided the server (`test-token` in this example):

```terminal
export CHROMA_CLIENT_AUTH_CREDENTIALS="test-token"
```

{% Tabs %}

{% Tab label="python" %}

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

{% /Tab %}

{% Tab label="typescript" %}

```typescript
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

{% /Tab %}

{% /Tabs %}

## Observability with AWS

Chroma is instrumented with [OpenTelemetry](https://opentelemetry.io/) hooks for observability. We currently only exports OpenTelemetry [traces](https://opentelemetry.io/docs/concepts/signals/traces/). These should allow you to understand how requests flow through the system and quickly identify bottlenecks.

Tracing is configured with four environment variables:

- `CHROMA_OTEL_COLLECTION_ENDPOINT`: where to send observability data. Example: `api.honeycomb.com`.
- `CHROMA_OTEL_SERVICE_NAME`: Service name for OTel traces. Default: `chromadb`.
- `CHROMA_OTEL_COLLECTION_HEADERS`: Headers to use when sending observability data. Often used to send API and app keys. For example `{"x-honeycomb-team": "abc"}`.
- `CHROMA_OTEL_GRANULARITY`: A value from the [OpenTelemetryGranularity enum](https://github.com/chroma-core/chroma/tree/main/chromadb/telemetry/opentelemetry/__init__.py). Specifies how detailed tracing should be.

To enable tracing on your Chroma server, simply pass your desired values as parameters when creating your Cloudformation stack:

```terminal
aws cloudformation create-stack --stack-name my-chroma-stack --template-url https://s3.amazonaws.com/public.trychroma.com/cloudformation/latest/chroma.cf.json \
 --parameters ParameterKey=ChromaOtelCollectionEndpoint,ParameterValue="api.honeycomb.com" \
 ParameterKey=ChromaOtelServiceName,ParameterValue="chromadb" \
 ParameterKey=ChromaOtelCollectionHeaders,ParameterValue="{'x-honeycomb-team': 'abc'}" \
 ParameterKey=ChromaOtelGranularity,ParameterValue="all" \
```

## Troubleshooting

#### Error: No default VPC for this user

If you get an error saying `No default VPC for this user` when creating `ChromaInstanceSecurityGroup`, head to [AWS VPC section](https://us-east-1.console.aws.amazon.com/vpc/home?region=us-east-1#vpcs) and create a default VPC for your user.