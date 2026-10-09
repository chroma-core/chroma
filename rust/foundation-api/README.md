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


## Pause creation during catalog migration

Set `foundation.provisioning_paused: true` in the Foundation API configuration to temporarily refuse both `POST /api/init` and `POST /api/tenants/{tenant}/foundations`. The hosted configuration nests this field at `foundation.foundation.provisioning_paused`. The default is `false`. Authorized callers receive HTTP 503 with a maintenance message before any catalog reservation, database creation, collection creation, or attached-function operation. The shared provisioning boundary applies the pause to internal and external callers alike. Existing reads and memory writes keep their normal behavior.

This configuration is read when the service starts. Deploy the paused configuration to every Foundation instance and wait for all older instances and their in-flight initialization requests to finish before taking the migration inventory. Keep provisioning paused while backfilling and replacing consumers. Reopen it only after every instance uses the catalog-aware implementation and the inventory has complete catalog coverage. Verify both creation endpoints return 503 while paused and resume normal behavior after reopening.

Production needs the pause before catalog-dependent consumers start serving traffic. First record the actual production image's hosted and OSS source revisions. Backport the configuration field and maintenance refusal onto that compatible source, placing the refusal before the first storage mutation in its initialization handler if it lacks the shared provisioning function. Test and deploy that compatible backport before pausing production. Do not deploy catalog-dependent main merely to obtain the pause. The catalog-aware release retains the same flag across the cutover.
