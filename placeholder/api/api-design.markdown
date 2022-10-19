# Chroma API Design

## Overview

Proposing an architecture for the overall Chroma service, with a split between external (logging and querying) and internal (service operation).

This document describes the HTTP API.

See client-design.markdown for description of the installable python client code.

## Logging API

Calls made inside the customer loop.

### High reliability

This doesn't need to be extreme from day one, but should at least be isolated from the other API calls (in a lightweight way for early versions).

#### Isolated, specific endpoints and services

http://log.trychroma.com/v1/

#### Respond quickly

For logging, we write the data to disk and return to the HTTP client ASAP.  We don't want to slow down their calls.

In the simplest case, e.g. a training record that expects no answer other than a confirmation, we simply respond with a 200.

But even in a more complex case, e.g. recording a prod record and expecting an assessment, if our internal service is unavailable, we respond quickly with a 202.

https://www.rfc-editor.org/rfc/rfc9110.html#name-202-accepted

#### Minimal dependencies

The customer input calls should buffer to committed (disk) storage and return.
