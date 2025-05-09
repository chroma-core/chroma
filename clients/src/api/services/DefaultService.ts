/* generated using openapi-typescript-codegen -- do not edit */
/* istanbul ignore file */
/* tslint:disable */
/* eslint-disable */
import type { AddCollectionRecordsPayload } from '../models/AddCollectionRecordsPayload';
import type { AddCollectionRecordsResponse } from '../models/AddCollectionRecordsResponse';
import type { ChecklistResponse } from '../models/ChecklistResponse';
import type { Collection } from '../models/Collection';
import type { CreateCollectionPayload } from '../models/CreateCollectionPayload';
import type { CreateDatabasePayload } from '../models/CreateDatabasePayload';
import type { CreateDatabaseResponse } from '../models/CreateDatabaseResponse';
import type { CreateTenantPayload } from '../models/CreateTenantPayload';
import type { CreateTenantResponse } from '../models/CreateTenantResponse';
import type { Database } from '../models/Database';
import type { DeleteCollectionRecordsPayload } from '../models/DeleteCollectionRecordsPayload';
import type { DeleteCollectionRecordsResponse } from '../models/DeleteCollectionRecordsResponse';
import type { DeleteDatabaseResponse } from '../models/DeleteDatabaseResponse';
import type { ForkCollectionPayload } from '../models/ForkCollectionPayload';
import type { GetRequestPayload } from '../models/GetRequestPayload';
import type { GetResponse } from '../models/GetResponse';
import type { GetTenantResponse } from '../models/GetTenantResponse';
import type { GetUserIdentityResponse } from '../models/GetUserIdentityResponse';
import type { HeartbeatResponse } from '../models/HeartbeatResponse';
import type { QueryRequestPayload } from '../models/QueryRequestPayload';
import type { QueryResponse } from '../models/QueryResponse';
import type { u32 } from '../models/u32';
import type { UpdateCollectionPayload } from '../models/UpdateCollectionPayload';
import type { UpdateCollectionRecordsPayload } from '../models/UpdateCollectionRecordsPayload';
import type { UpdateCollectionRecordsResponse } from '../models/UpdateCollectionRecordsResponse';
import type { UpdateCollectionResponse } from '../models/UpdateCollectionResponse';
import type { UpsertCollectionRecordsPayload } from '../models/UpsertCollectionRecordsPayload';
import type { UpsertCollectionRecordsResponse } from '../models/UpsertCollectionRecordsResponse';
import type { Vec } from '../models/Vec';
import type { CancelablePromise } from '../core/CancelablePromise';
import type { BaseHttpRequest } from '../core/BaseHttpRequest';
export class DefaultService {
    constructor(public readonly httpRequest: BaseHttpRequest) {}
    /**
     * Retrieves the current user's identity, tenant, and databases.
     * @returns GetUserIdentityResponse Get user identity
     * @throws ApiError
     */
    public getUserIdentity(): CancelablePromise<GetUserIdentityResponse> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/auth/identity',
            errors: {
                500: `Server error`,
            },
        });
    }
    /**
     * Health check endpoint that returns 200 if the server and executor are ready
     * @returns string Success
     * @throws ApiError
     */
    public healthcheck(): CancelablePromise<string> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/healthcheck',
            errors: {
                503: `Service Unavailable`,
            },
        });
    }
    /**
     * Heartbeat endpoint that returns a nanosecond timestamp of the current time.
     * @returns HeartbeatResponse Success
     * @throws ApiError
     */
    public heartbeat(): CancelablePromise<HeartbeatResponse> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/heartbeat',
            errors: {
                500: `Server error`,
            },
        });
    }
    /**
     * Pre-flight checks endpoint reporting basic readiness info.
     * @returns ChecklistResponse Pre flight checks
     * @throws ApiError
     */
    public preFlightChecks(): CancelablePromise<ChecklistResponse> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/pre-flight-checks',
            errors: {
                500: `Server error`,
            },
        });
    }
    /**
     * Reset endpoint allowing authorized users to reset the database.
     * @returns boolean Reset successful
     * @throws ApiError
     */
    public reset(): CancelablePromise<boolean> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/reset',
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Creates a new tenant.
     * @returns CreateTenantResponse Tenant created successfully
     * @throws ApiError
     */
    public createTenant({
        requestBody,
    }: {
        requestBody: CreateTenantPayload,
    }): CancelablePromise<CreateTenantResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants',
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Returns an existing tenant by name.
     * @returns GetTenantResponse Tenant found
     * @throws ApiError
     */
    public getTenant({
        tenantName,
    }: {
        /**
         * Tenant name or ID to retrieve
         */
        tenantName: string,
    }): CancelablePromise<GetTenantResponse> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant_name}',
            path: {
                'tenant_name': tenantName,
            },
            errors: {
                401: `Unauthorized`,
                404: `Tenant not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Lists all databases for a given tenant.
     * @returns Vec List of databases
     * @throws ApiError
     */
    public listDatabases({
        tenant,
        limit,
        offset,
    }: {
        /**
         * Tenant ID to list databases for
         */
        tenant: string,
        /**
         * Limit for pagination
         */
        limit?: number,
        /**
         * Offset for pagination
         */
        offset?: number,
    }): CancelablePromise<Vec> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant}/databases',
            path: {
                'tenant': tenant,
            },
            query: {
                'limit': limit,
                'offset': offset,
            },
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Creates a new database for a given tenant.
     * @returns CreateDatabaseResponse Database created successfully
     * @throws ApiError
     */
    public createDatabase({
        tenant,
        requestBody,
    }: {
        /**
         * Tenant ID to associate with the new database
         */
        tenant: string,
        requestBody: CreateDatabasePayload,
    }): CancelablePromise<CreateDatabaseResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases',
            path: {
                'tenant': tenant,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Retrieves a specific database by name.
     * @returns Database Database retrieved successfully
     * @throws ApiError
     */
    public getDatabase({
        tenant,
        database,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Name of the database to retrieve
         */
        database: string,
    }): CancelablePromise<Database> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant}/databases/{database}',
            path: {
                'tenant': tenant,
                'database': database,
            },
            errors: {
                401: `Unauthorized`,
                404: `Database not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Deletes a specific database.
     * @returns DeleteDatabaseResponse Database deleted successfully
     * @throws ApiError
     */
    public deleteDatabase({
        tenant,
        database,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Name of the database to delete
         */
        database: string,
    }): CancelablePromise<DeleteDatabaseResponse> {
        return this.httpRequest.request({
            method: 'DELETE',
            url: '/api/v2/tenants/{tenant}/databases/{database}',
            path: {
                'tenant': tenant,
                'database': database,
            },
            errors: {
                401: `Unauthorized`,
                404: `Database not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Lists all collections in the specified database.
     * @returns Vec List of collections
     * @throws ApiError
     */
    public listCollections({
        tenant,
        database,
        limit,
        offset,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name to list collections from
         */
        database: string,
        /**
         * Limit for pagination
         */
        limit?: number,
        /**
         * Offset for pagination
         */
        offset?: number,
    }): CancelablePromise<Vec> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections',
            path: {
                'tenant': tenant,
                'database': database,
            },
            query: {
                'limit': limit,
                'offset': offset,
            },
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Creates a new collection under the specified database.
     * @returns Collection Collection created successfully
     * @throws ApiError
     */
    public createCollection({
        tenant,
        database,
        requestBody,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name containing the new collection
         */
        database: string,
        requestBody: CreateCollectionPayload,
    }): CancelablePromise<Collection> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections',
            path: {
                'tenant': tenant,
                'database': database,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Retrieves a collection by ID or name.
     * @returns Collection Collection found
     * @throws ApiError
     */
    public getCollection({
        tenant,
        database,
        collectionId,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name
         */
        database: string,
        /**
         * UUID of the collection
         */
        collectionId: string,
    }): CancelablePromise<Collection> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Updates an existing collection's name or metadata.
     * @returns UpdateCollectionResponse Collection updated successfully
     * @throws ApiError
     */
    public updateCollection({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name
         */
        database: string,
        /**
         * UUID of the collection to update
         */
        collectionId: string,
        requestBody: UpdateCollectionPayload,
    }): CancelablePromise<UpdateCollectionResponse> {
        return this.httpRequest.request({
            method: 'PUT',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Deletes a collection in a given database.
     * @returns UpdateCollectionResponse Collection deleted successfully
     * @throws ApiError
     */
    public deleteCollection({
        tenant,
        database,
        collectionId,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name
         */
        database: string,
        /**
         * UUID of the collection to delete
         */
        collectionId: string,
    }): CancelablePromise<UpdateCollectionResponse> {
        return this.httpRequest.request({
            method: 'DELETE',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Adds records to a collection.
     * @returns AddCollectionRecordsResponse Collection added successfully
     * @throws ApiError
     */
    public collectionAdd({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        tenant: string,
        database: string,
        collectionId: string,
        requestBody: AddCollectionRecordsPayload,
    }): CancelablePromise<AddCollectionRecordsResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/add',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                400: `Invalid data for collection addition`,
            },
        });
    }
    /**
     * Retrieves the number of records in a collection.
     * @returns u32 Number of records in the collection
     * @throws ApiError
     */
    public collectionCount({
        tenant,
        database,
        collectionId,
    }: {
        /**
         * Tenant ID for the collection
         */
        tenant: string,
        /**
         * Database containing this collection
         */
        database: string,
        /**
         * Collection ID whose records are counted
         */
        collectionId: string,
    }): CancelablePromise<u32> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/count',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Deletes records in a collection. Can filter by IDs or metadata.
     * @returns DeleteCollectionRecordsResponse Records deleted successfully
     * @throws ApiError
     */
    public collectionDelete({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name
         */
        database: string,
        /**
         * Collection ID
         */
        collectionId: string,
        requestBody: DeleteCollectionRecordsPayload,
    }): CancelablePromise<DeleteCollectionRecordsResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/delete',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Forks an existing collection.
     * @returns Collection Collection forked successfully
     * @throws ApiError
     */
    public forkCollection({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name
         */
        database: string,
        /**
         * UUID of the collection to update
         */
        collectionId: string,
        requestBody: ForkCollectionPayload,
    }): CancelablePromise<Collection> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/fork',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Retrieves records from a collection by ID or metadata filter.
     * @returns GetResponse Records retrieved from the collection
     * @throws ApiError
     */
    public collectionGet({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name for the collection
         */
        database: string,
        /**
         * Collection ID to fetch records from
         */
        collectionId: string,
        requestBody: GetRequestPayload,
    }): CancelablePromise<GetResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/get',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Query a collection in a variety of ways, including vector search, metadata filtering, and full-text search
     * @returns QueryResponse Records matching the query
     * @throws ApiError
     */
    public collectionQuery({
        tenant,
        database,
        collectionId,
        requestBody,
        limit,
        offset,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name containing the collection
         */
        database: string,
        /**
         * Collection ID to query
         */
        collectionId: string,
        requestBody: QueryRequestPayload,
        /**
         * Limit for pagination
         */
        limit?: number,
        /**
         * Offset for pagination
         */
        offset?: number,
    }): CancelablePromise<QueryResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/query',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            query: {
                'limit': limit,
                'offset': offset,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Updates records in a collection by ID.
     * @returns UpdateCollectionRecordsResponse Collection updated successfully
     * @throws ApiError
     */
    public collectionUpdate({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        tenant: string,
        database: string,
        collectionId: string,
        requestBody: UpdateCollectionRecordsPayload,
    }): CancelablePromise<UpdateCollectionRecordsResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/update',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                404: `Collection not found`,
            },
        });
    }
    /**
     * Upserts records in a collection (create if not exists, otherwise update).
     * @returns UpsertCollectionRecordsResponse Records upserted successfully
     * @throws ApiError
     */
    public collectionUpsert({
        tenant,
        database,
        collectionId,
        requestBody,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name
         */
        database: string,
        /**
         * Collection ID
         */
        collectionId: string,
        requestBody: UpsertCollectionRecordsPayload,
    }): CancelablePromise<UpsertCollectionRecordsResponse> {
        return this.httpRequest.request({
            method: 'POST',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections/{collection_id}/upsert',
            path: {
                'tenant': tenant,
                'database': database,
                'collection_id': collectionId,
            },
            body: requestBody,
            mediaType: 'application/json',
            errors: {
                401: `Unauthorized`,
                404: `Collection not found`,
                500: `Server error`,
            },
        });
    }
    /**
     * Retrieves the total number of collections in a given database.
     * @returns u32 Count of collections
     * @throws ApiError
     */
    public countCollections({
        tenant,
        database,
    }: {
        /**
         * Tenant ID
         */
        tenant: string,
        /**
         * Database name to count collections from
         */
        database: string,
    }): CancelablePromise<u32> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/tenants/{tenant}/databases/{database}/collections_count',
            path: {
                'tenant': tenant,
                'database': database,
            },
            errors: {
                401: `Unauthorized`,
                500: `Server error`,
            },
        });
    }
    /**
     * Returns the version of the server.
     * @returns string Get server version
     * @throws ApiError
     */
    public version(): CancelablePromise<string> {
        return this.httpRequest.request({
            method: 'GET',
            url: '/api/v2/version',
        });
    }
}
