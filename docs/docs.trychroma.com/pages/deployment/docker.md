---
title: Docker
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

## Run Chroma in a Docker Container

You can run a Chroma server in a Docker container.

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

Start by creating a `.chroma_env` file. We will store in it various environment variables Chroma will need to enable authentication, and pass it to your container using the `--env-file` flag:

```sh
docker run --env-file ./.chroma_env -p 8000:8000 chromadb/chroma
```

### Basic Authentication

#### Server Set-Up

##### Generate Server-Side Credentials

{% note type="note" title="Security Practices" %}
A good security practice is to store the password securely. In the example below we use [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) (currently the only supported hash in Chroma server side auth) to hash the plaintext password.
{% /note %}

To generate the password hash, run the following command:

```bash
docker run --rm --entrypoint htpasswd httpd:2 -Bbn admin admin > server.htpasswd
```

This creates the bcrypt password hash for the password `admin`, for the `admin` user, and puts it into `server.htpasswd` in your current working directory. It will look like `admin:<password hash>`.

##### Running the Server

Set the following environment variables in `.chroma_env`:

```text
CHROMA_SERVER_AUTHN_CREDENTIALS=<contents of server.htpasswd>
CHROMA_SERVER_AUTHN_PROVIDER=chromadb.auth.basic_authn.BasicAuthenticationServerProvider
```

And run the Chroma container:

```bash
docker run --env-file ./.chroma_env -p 8000:8000 chromadb/chroma
```

#### Client Set-Up

Add the `CHROMA_CLIENT_AUTH_CREDENTIALS` environment variable to your `.chroma_en`, and set it to the user:password combination (`admin:admin` in this example):

```text
CHROMA_CLIENT_AUTH_CREDENTIALS=admin:admin
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

Install `python-dotenv`. This will allow us to read the environment variables from `.chroma_env` easily:

```shell
pip install python-dotenv
```

We will use Chroma's `Setting` object to define the authentication method on the client.

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

Try changing the user-password combination to be incorrect. The Chroma server will respond with a 403.

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

To use `X-Chroma-Token: test-token` type of authentication header you can set the `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` environment variable:

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

We will use Chroma's `Setting` object to define the authentication method on the client.

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
