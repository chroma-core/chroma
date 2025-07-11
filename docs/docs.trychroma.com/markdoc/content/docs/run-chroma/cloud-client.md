# Cloud Client

You can use the `CloudClient` to create a client connecting to Chroma Cloud.

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
client = CloudClient(
    tenant='Tenant ID',
    database='Database name',
    api_key='Chroma Cloud API key'
)
```
{% /Tab %}

{% Tab label="typescript" %}

```typescript
const client = new CloudClient({
    tenant: 'Tenant ID',
    database: 'Database name',
    apiKey: 'Chroma Cloud API key',
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

The `CloudClient` can be instantiated just with the API key argument. In which case, we will resolve the tenant and DB from Chroma Cloud. Note our auto-resolution will work only if the provided API key is scoped to a single DB.

If you set the `CHROMA_API_KEY`, `CHROMA_TENANT`, and the `CHROMA_DATABASE` environment variables, you can simply instantiate a `CloudClient` with no arguments:

{% TabbedCodeBlock %}

{% Tab label="python" %}
```python
client = CloudClient()
```
{% /Tab %}

{% Tab label="typescript" %}

```typescript
const client = new CloudClient();
```

{% /Tab %}

{% /TabbedCodeBlock %}