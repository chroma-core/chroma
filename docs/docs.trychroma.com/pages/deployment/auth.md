---
title: 🔒 Auth
---

{% tabs group="code-lang" hideContent=true %}

{% tab label="Python" %}
{% /tab %}

{% tab label="Javascript" %}
{% /tab %}

{% /tabs %}

You can configure Chroma to use authentication when in server/client mode only.

Supported authentication methods:

{% special_table %}
{% /special_table %}

| Authentication Method | Basic Auth (Pre-emptive)                                                                                                  | Static API Token                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Description           | [RFC 7617](https://www.rfc-editor.org/rfc/rfc7617) Basic Auth with `user:password` base64-encoded `Authorization` header. | Static auth token in `Authorization: Bearer <token>` or in `X-Chroma-Token: <token>` headers. |
| Status                | `Alpha`                                                                                                                   | `Alpha`                                                                                       |
| Server-Side Support   | ✅ `Alpha`                                                                                                                | ✅ `Alpha`                                                                                    |
| Client/Python         | ✅ `Alpha`                                                                                                                | ✅ `Alpha`                                                                                    |
| Client/JS             | ✅ `Alpha`                                                                                                                | ✅ `Alpha`                                                                                    |

***

In this guide we will add authentication to a simple Chroma server running locally using our CLI:

```shell
chroma run --path <DB path>
```

We also have dedicated auth guides for various deployments:
* [Docker](/deployment/docker#authentication-with-docker)
* More coming soon!

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

Set the following environment variables:

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS_FILE="<path to server.htpasswd>"
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.basic_authn.BasicAuthenticationServerProvider"
```

And run the Chroma server:

```bash
chroma run --path <DB path>
```

#### Client Set-Up

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

We will use Chroma's `Setting` object to define the authentication method on the client.

```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient(
    host="localhost",
    port=8000,
    settings=Settings(
        chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",
        chroma_client_auth_credentials="admin:admin"
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
        credentials: "admin:admin"
    }
})

chromaClient.heartbeat()
```

{% /tab %}
{% /tabs %}

We recommend setting the environment variable `CHROMA_CLIENT_AUTH_CREDENTIALS` instead of specifying the credentials in code.

### Static API Token Authentication

#### Server Set-Up

{% note type="note" title="Security Note" %}
Current implementation of static API token auth supports only ENV based tokens. Tokens must be alphanumeric ASCII strings. Tokens are case-sensitive.
{% /note %}

If, for example, you want the static API token to be "test-token", set the following environment variables. This will set `Authorization: Bearer test-token` as your authentication header.

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS="test-token"
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
```

To use `X-Chroma-Token: test-token` type of authentication header you can set the `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` environment variable:

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS="test-token"
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
export CHROMA_AUTH_TOKEN_TRANSPORT_HEADER="X-Chroma-Token"
```

Then, run the Chroma server:

```bash
chroma run --path <DB path>
```

To configure multiple tokens and use them for role-based access control (RBAC), use a file like [this](https://github.com/chroma-core/chroma/blob/main/examples/basic_functionality/authz/authz.yaml) and the following environment variables:

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS_FILE="<path_to_authz.yaml>"
export CHROMA_SERVER_AUTHZ_CONFIG_FILE="<path_to_authz.yaml>"  # Note: these are the same!
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
export CHROMA_SERVER_AUTHZ_PROVIDER="chromadb.auth.simple_rbac_authz.SimpleRBACAuthorizationProvider"
```

#### Client Set-Up

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

We will use Chroma's `Setting` object to define the authentication method on the client.

```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient(
    host="localhost",
    port=8000,
    settings=Settings(
        chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
        chroma_client_auth_credentials="test-token"
    )
)

chroma_client.heartbeat()
```

If you are using a custom `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` (like `X-Chroma-Token`), add it to your `Settings`:

```python
chroma_auth_token_transport_header="X-Chroma-Token"
```

{% /tab %}
{% tab label="Javascript" %}

```javascript
import { ChromaClient } from "chromadb";

const chromaClient = new ChromaClient({ 
    path: "http://localhost:8000", 
    auth: {
        provider: "token",
        credentials: "test-token",
        tokenHeaderType: "X-Chroma-Token"
    }
})

chromaClient.heartbeat()
```

{% /tab %}
{% /tabs %}

We recommend setting the environment variable `CHROMA_CLIENT_AUTH_CREDENTIALS` instead of specifying the token in code. Similarly, you can read the value of `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` in the client construction.
