---
title: 🔒 Auth
---


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

### Basic Authentication

#### Server Setup

##### Generate Server-Side Credentials

{% note type="note" title="Security Practices" %}
A good security practice is to store the password securely. In the example below we use [bcrypt](https://en.wikipedia.org/wiki/Bcrypt) (currently the only supported hash in Chroma server side auth) to hash the plaintext password.
{% /note %}

To generate the password hash, run the following command:

```bash
docker run --rm --entrypoint htpasswd httpd:2 -Bbn admin admin > server.htpasswd
```

This creates the bcrypt password hash for the password `admin` and puts it into `server.htpasswd` alongside the user `admin`. It will look like `admin:<password hash>`.

##### Running the Server

Set the following environment variables:

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS_FILE="server.htpasswd"
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.basic_authn.BasicAuthenticationServerProvider"
```

And run the server as normal:

```bash
chroma run --path /db_path
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

#### Client Setup (Python)


```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient(
  settings=Settings(chroma_client_auth_provider="chromadb.auth.basic_authn.BasicAuthClientProvider",chroma_client_auth_credentials="admin:admin"))
client.heartbeat()  # this should work with or without authentication - it is a public endpoint

client.get_version()  # this should work with or without authentication - it is a public endpoint

client.list_collections()  # this is a protected endpoint and requires authentication
```

{% /tab %}
{% tab label="Javascript" %}

#### Client Setup (JavaScript/TypeScript)

##### Basic authentication (username & password)
```javascript
const client = new ChromaClient({
  path: "http://localhost:8000"
  auth: { provider: "basic", credentials: "admin:admin" },
});
```

##### Token authentication
In this method, we use the Bearer scheme. Namely, the token is sent as: `Authorization: Bearer test-token`
```javascript
const client = new ChromaClient({
  path: "http://localhost:8000",
  auth: { provider: "token", credentials: "test-token" },
});
```

##### Token authentication (custom header)
In this method, we send the token in a custom header. The header is `X-Chroma-Token`.
```javascript
const client = new ChromaClient({
  path: URL,
  auth: {
    provider: "token",
    credentials: "test-token",
    tokenHeaderType: "X_CHROMA_TOKEN",
  },
});
```


{% /tab %}

{% /tabs %}

### Static API Token Authentication

{% note type="note" title="Tokens" %}
Tokens must be alphanumeric ASCII strings. Tokens are case-sensitive.
{% /note %}

#### Server Setup

{% note type="note" title="Security Note" %}
Current implementation of static API token auth supports only ENV based tokens.
{% /note %}

##### Running the Server

Set the following environment variables to use `Authorization: Bearer test-token` to be your authentication header. All environment variables can also be set as [Settings](https://docs.trychroma.com/deployment/aws#step-5:-configure-the-chroma-library).

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS="test-token"
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
```

To configure multiple tokens and use them for role-based access control (RBAC), use a file like [this](https://github.com/chroma-core/chroma/blob/main/examples/basic_functionality/authz/authz.yaml) and the following configuration settings:

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS_FILE=<path_to_authz.yaml>
export CHROMA_SERVER_AUTHZ_CONFIG_FILE=<path_to_authz.yaml>  # Note: these are the same!
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
export CHROMA_SERVER_AUTHZ_PROVIDER="chromadb.auth.simple_rbac_authz.SimpleRBACAuthorizationProvider"
```

To use `X-Chroma-Token: test-token` type of authentication header you can set the `CHROMA_AUTH_TOKEN_TRANSPORT_HEADER` environment variable or configuration setting.

```bash
export CHROMA_SERVER_AUTHN_CREDENTIALS="test-token"
export CHROMA_SERVER_AUTHN_PROVIDER="chromadb.auth.token_authn.TokenAuthenticationServerProvider"
export CHROMA_AUTH_TOKEN_TRANSPORT_HEADER="X_CHROMA_TOKEN"

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

#### Client Setup

```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient(
    settings=Settings(chroma_client_auth_provider="chromadb.auth.token_authn.TokenAuthClientProvider",
                      chroma_client_auth_credentials="test-token"))
client.heartbeat()  # this should work with or without authentication - it is a public endpoint

client.get_version()  # this should work with or without authentication - it is a public endpoint

client.list_collections()  # this is a protected endpoint and requires authentication
```

{% /tab %}
{% tab label="Javascript" %}

#### Client Setup

Using the default `Authorization: Bearer <token>` header:

```js
import { ChromaClient } from "chromadb";

const client = new ChromaClient({
  auth: { provider: "token", credentials: "test-token" },
});
//or explicitly specifying the auth header type
const client = new ChromaClient({
  auth: {
    provider: "token",
    credentials: "test-token",
    tokenHeaderType: "AUTHORIZATION",
  },
});
```

Using custom Chroma auth token `X-Chroma-Token: <token>` header:

```js
import { ChromaClient } from "chromadb";

const client = new ChromaClient({
  auth: {
    provider: "token",
    credentials: "test-token",
    tokenHeaderType: "X_CHROMA_TOKEN",
  },
});
```


{% /tab %}

{% /tabs %}
