---
id: cli-db
name: DB Management
---

# DB Management on Chroma Cloud

The Chroma CLI lets you interact with your Chroma Cloud databases for your active [profile](./profile).

### Connect

The `connect` command will output a connection code snippet for your Chroma Cloud database in Python or JS/TS. If you don't provide the `name` or `language` the CLI will prompt you to choose your preferences. The `name` argument is always assumed to be the first, so you don't need to include the `--name` flag.

The output code snippet will already have the API key of your profile set for the client construction.

```terminal
chroma db connect [db_name] [--language python/JS/TS]
```

The `connect` command can also add Chroma environment variables (`CHROMA_API_KEY`, `CHROMA_TENANT`, and `CHROMA_DATABASE`) to a `.env` file in your current working directory. It will create a `.env` file for you if it doesn't exist:

```terminal
chroma db connect [db_name] --env-file
```

If you prefer to simply output these variables to your terminal use:

```terminal
chroma db connect [db_name] --env-vars
```

Setting these environment variables will allow you to concisely instantiate the `CloudClient` with no arguments.

### Create

The `create` command lets you create a database on Chroma Cloud. It has the `name` argument, which is the name of the DB you want to create. If you don't provide it, the CLI will prompt you to choose a name.

If a DB with your provided name already exists, the CLI will error.

```terminal
chroma db create my-new-db
```

### Delete

The `delete` command deletes a Chroma Cloud DB. Use this command with caution as deleting a DB cannot be undone. The CLI will ask you to confirm that you want to delete the DB with the `name` you provided.

```terminal
chroma db delete my-db
```

### List

The `list` command lists all the DBs you have under your current profile.

```terminal
chroma db list
```
