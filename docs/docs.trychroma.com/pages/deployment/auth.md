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
A good security practice is to store the password securely. In the example below we use bcrypt (currently the only supported hash in Chroma server side auth) to hash the plaintext password.
{% /note %}

To generate the password hash, run the following command. Note that you will need to have `htpasswd` installed on your system.

```bash
htpasswd -Bbn admin admin > server.htpasswd
```

##### Running the Server

Set the following environment variables:

```bash
export CHROMA_SERVER_AUTH_CREDENTIALS_FILE="server.htpasswd"
export CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.providers.HtpasswdFileServerAuthCredentialsProvider"
export CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.basic.BasicAuthServerProvider"
```

And run the server as normal:

```bash
chroma run --path /db_path
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

#### Client Setup

```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient(
  settings=Settings(chroma_client_auth_provider="chromadb.auth.basic.BasicAuthClientProvider",chroma_client_auth_credentials="admin:admin"))
client.heartbeat()  # this should work with or without authentication - it is a public endpoint

client.get_version()  # this should work with or without authentication - it is a public endpoint

client.list_collections()  # this is a protected endpoint and requires authentication
```

{% /tab %}
{% tab label="Javascript" %}

#### Client Setup

```js
import { ChromaClient } from "chromadb";

const client = new ChromaClient({
  auth: { provider: "basic", credentials: "admin:admin" },
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

Set the following environment variables to use `Authorization: Bearer test-token` to be your authentication header.

```bash
export CHROMA_SERVER_AUTH_CREDENTIALS="test-token"
export CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"
export CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.token.TokenAuthServerProvider"
```

to use `X-Chroma-Token: test-token` type of authentication header you can set an additional environment variable.

```bash
export CHROMA_SERVER_AUTH_CREDENTIALS="test-token"
export CHROMA_SERVER_AUTH_CREDENTIALS_PROVIDER="chromadb.auth.token.TokenConfigServerAuthCredentialsProvider"
export CHROMA_SERVER_AUTH_PROVIDER="chromadb.auth.token.TokenAuthServerProvider"
export CHROMA_SERVER_AUTH_TOKEN_TRANSPORT_HEADER="X_CHROMA_TOKEN"
```

{% tabs group="code-lang" hideTabs=true %}
{% tab label="Python" %}

#### Client Setup

```python
import chromadb
from chromadb.config import Settings

client = chromadb.HttpClient(
    settings=Settings(chroma_client_auth_provider="chromadb.auth.token.TokenAuthClientProvider",
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
    providerOptions: { headerType: "AUTHORIZATION" },
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
    providerOptions: { headerType: "X_CHROMA_TOKEN" },
  },
});
```


{% /tab %}

{% /tabs %}
