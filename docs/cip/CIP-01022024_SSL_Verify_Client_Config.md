# CIP-01022024 Allow Filtering for Collections

## Status

Current Status: `Under Discussion`

## Motivation

The motivation for this change is to enhance security and flexibility in Chroma's client API. Users need the ability to
configure SSL contexts to trust custom CA certificates or self-signed certificates, which is not straightforward with
the current setup. This capability is crucial for organizations that operate their own CA or for developers who need to
test their applications in environments where certificates from a recognized CA are not available or practical.

## Public Interfaces

New settings variable `chroma_server_ssl_verify` accepting either a boolean or a path to a certificate file. If the
value is a path to a certificate file, the file will be used to verify the server's certificate. The value is passed
as `verify` parameter to `requests.Session` of the `FastAPI` client.

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
