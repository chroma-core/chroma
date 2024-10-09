---
title: Docker
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

## Run Chroma in a Docker Container

You can run a Chroma server in a Docker container, and access it using the `HttpClient`.

If you are using Chroma in production, please fill out [this form](https://airtable.com/appqd02UuQXCK5AuY/pagr1D0NFQoNpUpNZ/form), and we will add you to a dedicated Slack workspace for supporting production users. We would love to help you think through the design of your system, or if you would be a good fit for our upcoming distributed cloud service. You can also join the [#production-chroma](https://discord.com/channels/1073293645303795742/1292554909694300211) channel on Discord to join our community!

If you are using a client in a separate container from the one running your Chroma server, you may only need the [thin-client package](./thin-client)

You can get the Chroma Docker image from [Docker Hub](https://hub.docker.com/r/chromadb/chroma), or from the [Chroma GitHub Container Registry](https://github.com/chroma-core/chroma/pkgs/container/chroma)

```sh
docker pull chromadb/chroma
docker run -p 8000:8000 chromadb/chroma
```

You can also build the Docker image yourself from the Dockerfile in the [Chroma GitHub repository](https://github.com/chroma-core/chroma)

```sh
git clone git@github.com:chroma-core/chroma.git
cd chroma
docker-compose up -d --build
```

The Chroma client can then be configured to connect to the server running in the Docker container.

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

```python
import chromadb
chroma_client = chromadb.HttpClient(host='localhost', port=8000)
chroma_client.heartbeat()
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({ path: "http://localhost:8000" })
chromaClient.heartbeat()
```

{% /tab %}
{% /tabs %}

## Authentication with Docker

By default, the Docker image will run with no authentication. In client/server mode, Chroma supports the following authentication methods:
* [RFC 7617](https://www.rfc-editor.org/rfc/rfc7617) Basic Auth with `user:password` base64-encoded `Authorization` header.
* Static auth token in `Authorization: Bearer <token>` or in `X-Chroma-Token: <token>` headers.

You can learn more about authentication with Chroma in the [Auth Guide](/deployment/auth).

### Encrypted User:Password Authentication

#### Server Set-Up

##### Generate Server-Side Credentials

{% note type="note" title="Security Practices" %}
A good security practice is to store the password securely. In the example below we use [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) (currently the only supported hash in Chroma server side auth) to hash the plaintext password.  If you'd like to see support for additional hash functions, feel free to [contribute](../contributing) new ones!
{% /note %}

To generate the password hash, run the following command:

```bash
docker run --rm --entrypoint htpasswd httpd:2 -Bbn admin admin > server.htpasswd
```

This creates the bcrypt password hash for the password `admin`, for the `admin` user, and puts it into `server.htpasswd` in your current working directory. It will look like `admin:<password hash>`.

##### Running the Server

Create a `.chroma_env` file, and set in it the following environment variables:

```text
CHROMA_SERVER_AUTHN_CREDENTIALS=<contents of server.htpasswd>
CHROMA_SERVER_AUTHN_PROVIDER=chromadb.auth.basic_authn.BasicAuthenticationServerProvider
```

Then, run the Chroma container, and pass it your `.chroma_env` using the `--env-file` flag:

```bash
docker run --env-file ./.chroma_env -p 8000:8000 chromadb/chroma
```

#### Client Set-Up

In your client environment, set the `CHROMA_CLIENT_AUTH_CREDENTIALS` variable to the user:password combination (`admin:admin` in this example):

```shell
export CHROMA_CLIENT_AUTH_CREDENTIALS="admin:admin"
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Install `python-dotenv`. This will allow us to read the environment variables from `.chroma_env` easily:

```shell
pip install python-dotenv
```

We will use Chroma's `Settings` object to define the authentication method on the client.

```python
import os
import chromadb
from chromadb.config import Settings
from dotenv import load_dotenv

load_dotenv('/path/to/your/.chroma_env')

client = chromadb.HttpClient(
    host="localhost",
    port=8000,
    settings=Settings(
        chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
        chroma_client_auth_credentials=os.getenv("CHROMA_CLIENT_AUTH_CREDENTIALS")
    )
)

chroma_client.heartbeat()
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({
    path: "http://localhost:8000",
    auth: {
        provider: "basic",
        credentials: process.env.CHROMA_CLIENT_AUTH_CREDENTIALS
    }
})

chromaClient.heartbeat()
```

{% /tab %}
{% /tabs %}

### Static API Token Authentication

#### Server Set-Up

{% note type="note" title="Security Note" %}
Current implementation of static API token auth supports only ENV based tokens. Tokens must be alphanumeric ASCII strings. Tokens are case-sensitive.
{% /note %}

If, for example, you want the static API token to be "test-token", add the following environment variables to your `.chroma_env`. This will set `Authorization: Bearer test-token` as your authentication header.

```text
CHROMA_SERVER_AUTHN_CREDENTIALS=test-token
CHROMA_SERVER_AUTHN_PROVIDER=chromadb.auth.token_authn.TokenAuthenticationServerProvider
```

If instead of the default `Authorization: Bearer <token>` header, you want to use a custom one like `X-Chroma-Token: test-token`, you can set the `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` environment variable:

```text
CHROMA_SERVER_AUTHN_CREDENTIALS=test-token
CHROMA_SERVER_AUTHN_PROVIDER=chromadb.auth.token_authn.TokenAuthenticationServerProvider
CHROMA_AUTH_TOKEN_TRANSPORT_HEADER=X-Chroma-Token
```

Then, run the Chroma server:

```bash
docker run --env-file ./.chroma_env -p 8000:8000 chromadb/chroma
```

To configure multiple tokens and use them for role-based access control (RBAC), use a file like [this](https://github.com/chroma-core/chroma/blob/main/examples/basic_functionality/authz/authz.yaml) and the following environment variables:

```text
CHROMA_SERVER_AUTHN_CREDENTIALS_FILE=<path_to_authz.yaml>
CHROMA_SERVER_AUTHZ_CONFIG_FILE=<path_to_authz.yaml>  # Note: these are the same!
CHROMA_SERVER_AUTHN_PROVIDER=chromadb.auth.token_authn.TokenAuthenticationServerProvider
CHROMA_SERVER_AUTHZ_PROVIDER=chromadb.auth.simple_rbac_authz.SimpleRBACAuthorizationProvider
```

In this case, you will have to set up a volume to allow the Chroma Docker container to use your `authz.yaml` file:

```bash
docker run --env-file ./.chroma_env -v <path_to_authz.yaml>:/chroma/<authz.yaml> -p 8000:8000 chromadb/chroma
```

#### Client Set-Up

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Install `python-dotenv`. This will allow us to read the environment variables from `.chroma_env` easily:

```shell
pip install python-dotenv
```

We will use Chroma's `Settings` object to define the authentication method on the client.

```python
import os
import chromadb
from chromadb.config import Settings
from dotenv import load_dotenv

load_dotenv('/path/to/your/.chroma_env')

client = chromadb.HttpClient(
    host="localhost",
    port=8000,
    settings=Settings(
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials=os.getenv("CHROMA_CLIENT_AUTH_CREDENTIALS")
    )
)

chroma_client.heartbeat()
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
    path: "http://localhost:8000",
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
