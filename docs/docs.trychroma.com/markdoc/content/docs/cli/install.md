---
id: cli-install
name: Installing the CLI
---

# Installing the Chroma CLI

The Chroma CLI lets you run a Chroma server locally on your machine, install sample apps, browse your collections, interact with your Chroma Cloud DBs, and much more!

When you install our Python or JavaScript package globally, you will automatically get the Chroma CLI.

If you don't use one of our packages, you can still install the CLI as a standalone program with `cURL` (or `iex` on Windows).

## Python

You can install Chroma using `pip`:

```terminal
pip install chromadb
```

If you're machine does not allow for global `pip` installs, you can get the Chroma CLI with `pipx`:

```terminal
pipx install chromadb
```

## JavaScript

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="yarn" %}

```terminal
yarn global add chromadb
```

{% /Tab %}

{% Tab label="npm" %}

```terminal
npm install -g chromadb
```

{% /Tab %}

{% Tab label="pnpm" %}

```terminal
pnpm add -g chromadb
```

{% /Tab %}

{% Tab label="bun" %}

```terminal
bun add -g chromadb
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

## Install Globally

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="cURL" %}

```terminal
curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash
```

{% /Tab %}

{% Tab label="Windows" %}

```terminal
iex ((New-Object System.Net.WebClient).DownloadString('https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.ps1'))
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}
