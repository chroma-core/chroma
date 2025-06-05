# Cloud Client

{% Banner type="tip" %}
Python support coming soon!
{% /Banner %}

You can use the `CloudClient` to create a client connecting to Chroma Cloud.

```typescript
const client = new CloudClient({
    apiKey: 'Chroma Cloud API key',
    tenant: 'Tenant ID',
    database: 'Database name'
});
```

The `CloudClient` can be instantiated just with the `apiKey` argument. In which case, it will resolve the tenant and DB from Chroma Cloud.

If you set the `CHROMA_API_KEY`, `CHROMA_TENANT`, and the `CHROMA_DATABASE` environment variables, you can simply instantiate a `CloudClient` with no arguments:

```typescript
const client = new CloudClient();
```