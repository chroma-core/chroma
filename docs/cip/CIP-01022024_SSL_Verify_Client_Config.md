# CIP-01022024 SSL Verify Client Config

## Status

Current Status: `Under Discussion`

## Motivation

The motivation for this change is to enhance security and flexibility in Chroma's client API. Users need the ability to
configure SSL contexts to trust custom CA certificates or self-signed certificates, which is not straightforward with
the current setup. This capability is crucial for organizations that operate their own CA or for developers who need to
test their applications in environments where certificates from a recognized CA are not available or practical.

The suggested change entails a server-side certificate be available, but this CIP does not prescribe how such
certificate should be configured or obtained. In our testing, we used a self-signed certificate generated with
`openssl` and configured the client to trust the certificate. We also experiment with a SSL-terminated proxy server.
Both of approaches yielded the same results.

> **IMPORTANT:** It should be noted that we do not recommend or encourage the use of self-signed certificates in
> production environments.

We also provide a sample notebook that to help the reader run a local Chroma server with a self-signed certificate and
configure the client to trust the certificate. The notebook can be found
in [assets/CIP-01022024-test_self_signed.ipynb](./assets/CIP-01022024-test_self_signed.ipynb).

## Public Interfaces

> **Note:** The following changes are only applicable to Chroma HttpClient.

New settings variable `chroma_server_ssl_verify` accepting either a boolean or a path to a certificate file. If the
value is a path to a certificate file, the file will be used to verify the server's certificate. If the value is a
boolean, the SSL certificate verification can be bypassed (`false`) or enforced (`true`).

The value is passed as `verify` parameter to `requests.Session` of the `FastAPI` client. See
requests [documentation](https://requests.readthedocs.io/en/latest/user/advanced/#ssl-cert-verification) for
more details.

Example Usage:

```python
import chromadb
from chromadb import Settings
client = chromadb.HttpClient(host="localhost",port="8443",ssl=True, settings=Settings(chroma_server_ssl_verify='./servercert.pem'))
# or with boolean
client = chromadb.HttpClient(host="localhost",port="8443",ssl=True, settings=Settings(chroma_server_ssl_verify=False))
```

### Resources

- https://requests.readthedocs.io/en/latest/api/#requests.request
- https://www.geeksforgeeks.org/ssl-certificate-verification-python-requests/

## Proposed Changes

The proposed changes are mentioned in the public interfaces.

## Compatibility, Deprecation, and Migration Plan

The change is not backward compatible from client's perspective as the lack of the feature in prior clients will cause
an error when passing the new settings parameter. Server-side is not affected by this change.

## Test Plan

API tests with SSL verification enabled and a self-signed certificate.

## Rejected Alternatives

N/A
