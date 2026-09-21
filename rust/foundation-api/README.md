# Foundation product registry

A Foundation is a product resource backed by one Chroma database. Its identity and lifecycle live in the product's registry, outside Chroma's system database. A collection named `wiki` is storage belonging to a Foundation and never establishes its identity.

The host supplies a caller-authorized implementation of `FoundationRegistry` through `FoundationApiServer::with_foundation_registry` or `foundation_service_entrypoint_with_foundation_registry`. The standalone entrypoints install `UnconfiguredRegistry`, so catalog-dependent operations fail with HTTP 503 until a host supplies an adapter. Tests explicitly supply an in-memory registry. The standalone service never infers catalog records from collections.

## Creation and recovery

Named creation checks database-creation and Foundation-initialization permissions, reserves a product record and backing database UUID, provisions storage with that UUID, and marks the record ready after all collections and attached functions succeed. A retry resumes the same identities. An existing database with the same name and another UUID produces a conflict. A provisioning failure leaves the reservation available for retry, including a failure to record completion after storage succeeds. Retries also resume an attachment interrupted between creation and its finish operation. An existing ready attachment is left unchanged.

The default initialization route can adopt a database that a first-party client creates before initialization. It first reads that database through the Chroma frontend using the caller's token, then submits the observed UUID to the registry. Only the configured default name is eligible. The registry must reject any observation that disagrees with an existing product record. A ready Foundation whose storage is missing is unavailable; initialization does not recreate its storage or attach it to a replacement database.

The direct system-database writes in provisioning create Chroma storage and attached functions. They never create or store the product's Foundation record.

## Addressing and access

Named memory requests use `/api/tenants/{tenant}/foundations/{foundation}/...`. Creation and listing use `/api/tenants/{tenant}/foundations`; describe uses `/api/tenants/{tenant}/foundations/{foundation}`. MCP clients address a named Foundation at `/mcp/tenants/{tenant}/foundations/{foundation}`. Existing unprefixed API paths and `/mcp/foundation` select the caller's configured default Foundation. Initialization remains at `/api/init`.

The tenant and Foundation path components retain their existing allowlists. The product reserves the Foundation name `foundations` independently of route matching.

Catalog reads carry the original caller's headers. The registry applies the canonical Foundation permissions and effective database scope before pagination. Listing accepts `limit` from 1 through 100 and a nonnegative `offset`; `next_offset` is null at the end. Authorization or service failures remain errors and never become an empty page.

Memory routes, including default aliases and MCP tool calls, require a ready registered Foundation and verify the backing database UUID through the caller-authorized frontend before accessing memory. A cached collection cannot bypass this guard. Describe returns the registered identity even when its storage is missing, with `storage_available: false`. This field checks the backing database's identity; it does not certify the health of every collection. Removing `wiki` leaves the Foundation identity intact.

Data-plane record operations still address a database by name. The UUID check rejects a database name that already refers to replacement storage, but the frontend does not atomically bind that check to every subsequent operation. Concurrent deletion and recreation between the check and operation remains a storage-protocol limitation. Name reuse is unsupported; reconciliation must preserve the old identity and must never silently rebind it.

## Deployment prerequisite

Before routing traffic to these consumers, deploy the product catalog, install its hosted adapter, and register existing Foundations. Every active default Foundation also needs a catalog record. There is no fallback to collection discovery during a catalog outage. The product control plane owns migration, reconciliation, catalog access policy, and the registrar's service credential.
