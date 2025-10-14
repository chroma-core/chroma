---
id: cli-profile
name: Profile Management
---

# Profile Management

A **profile** in the Chroma CLI persists the credentials (API key and tenant ID) for authenticating with Chroma Cloud.

Each time you use the [`login`](./login) command, the CLI will create a profile for the team you logged in with. All profiles are saved in the `.chroma/credentials` file in your home directory.

The CLI also keeps track of your "active" profile in `.chroma/config.json`. This is the profile that will be used for all CLI commands with Chroma Cloud. For example, if you [logged](./login) into your "staging" team on Chroma Cloud, and set it as your active profile. Later, when you use the `chroma db create my-db` command, you will see `my-db` created under your "staging" team.

The `profile` command lets you manage your profiles.

### Delete

Deletes a profile. The CLI will ask you to confirm if you are trying to delete your active profile. If this is the case, be sure to use the `profile use` command to set a new active profile, otherwise all future Chrom Cloud CLI commands will fail.

```terminal
chroma profile delete [profile_name]
```

### List

Lists all your available profiles

```terminal
chroma profile list
```

### Show

Outputs the name of your active profile

```termnial
chroma profile show
```

### Rename

Rename a profile

```termnial
chroma profile rename [old_name] [new_name]
```

### Use

Set a new profile as the active profile

```terminal
chroma profile use [profile_name]
```
