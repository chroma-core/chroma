# Copy Chroma Collections

Using the Chroma CLI, you can copy collections from a local Chroma server to Chroma Cloud and vice versa.

```terminal
chroma copy --from-local collections [collection names] 
```

### Arguments

* `collections` - Space separated list of the names of the collections you want to copy. Conflicts with `all`.
* `all` - Instructs the CLI to copy all collections from the source DB.
* `from-local` - Sets the copy source to a local Chroma server. By default, the CLI will try to find it at `localhost:8000`. If you have a different setup, use `path` or `host`.
* `from-cloud` - Sets the copy source to a DB on Chroma Cloud.
* `to-local` - Sets the copy target to a local Chroma server. By default, the CLI will try to find it at `localhost:8000`. If you have a different setup, use `path` or `host`.
* `to-cloud` - Sets the copy target to a DB on Chroma Cloud.
* `db` - The name of the Chroma Cloud DB with the collections you want to copy. If not provided, the CLI will prompt you to select a DB from those available on your active [profile](./profile).
* `host` - The host of your local Chroma server. This argument conflicts with `path`.
* `path` - The path of your local Chroma data. If provided, the CLI will use the data path to start a local Chroma server at an available port for browsing. This argument conflicts wth `host`.

### Copy from Local to Chroma Cloud

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="simple" %}
```terminal
chroma copy --from-local collections col-1 col-2 
```
{% /Tab %}

{% Tab label="with DB" %}
```terminal
chroma copy --from-local --all --db my-db
```

{% /Tab %}

{% Tab label="host" %}
```terminal
chroma copy --from-local --all --host http://localhost:8050 
```
{% /Tab %}

{% Tab label="path" %}
```terminal
chroma copy --from-local --all --path ~/Developer/my-app/chroma 
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

### Copy from Chroma Cloud to Local

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="simple" %}
```terminal
chroma copy --from-cloud collections col-1 col-2 
```
{% /Tab %}

{% Tab label="with DB" %}
```terminal
chroma copy --from-cloud --all --db my-db
```

{% /Tab %}

{% Tab label="host" %}
```terminal
chroma copy --from-cloud --all --host http://localhost:8050 
```
{% /Tab %}

{% Tab label="path" %}
```terminal
chroma copy --from-cloud --all --path ~/Developer/my-app/chroma 
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

### Quotas

You may run into quota limitations when copying local collections to Chroma Cloud, for example if the size of your metadata values on records is too large. If the CLI notifies you that a quota has been exceeded, you can request an increase on the Chroma Cloud dashboard. Click "Settings" on your active profile's team, and then choose the "Quotas" tab.