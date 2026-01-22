# Open Source Chroma

**Chroma is the open-source AI application database**. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

## Contributing

We welcome all contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas.

### Getting Started

Here are some helpful links to get you started with contributing to Chroma

- The Chroma codebase is hosted on [Github](https://github.com/chroma-core/chroma)
- Issues are tracked on [Github Issues](https://github.com/chroma-core/chroma/issues). Please report any issues you find there making sure to fill out the correct [form for the type of issue you are reporting](https://github.com/chroma-core/chroma/issues/new/choose).
- In order to run Chroma locally you can follow the [Development Instructions](https://github.com/chroma-core/chroma/blob/main/DEVELOP.md).
- If you want to contribute and aren't sure where to get started you can search for issues with the [Good first issue](https://github.com/chroma-core/chroma/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) tag or take a look at our [Roadmap](https://docs.trychroma.com/roadmap).
- The Chroma documentation (including this page!) is hosted on [Github](https://github.com/chroma-core/chroma/tree/main/docs) as well. If you find any issues with the documentation please report them on the Github Issues page for [the documentation](https://github.com/chroma-core/chroma/issues).

### Contributing Code and Ideas

#### Pull Requests

In order to submit a change to Chroma please submit a [Pull Request](https://github.com/chroma-core/chroma/compare) against Chroma or the documentation. The pull request will be reviewed by the Chroma team and if approved, will be merged into the repository. We will do our best to review pull requests in a timely manner but please be patient as we are a small team. We will work to integrate your proposed changes as quickly as possible if they align with the goals of the project. We ask that you label your pull request with a title prefix that indicates the type of change you are proposing. The following prefixes are used:

```
ENH: Enhancement, new functionality
BUG: Bug fix
DOC: Additions/updates to documentation
TST: Additions/updates to tests
BLD: Updates to the build process/scripts
PERF: Performance improvement
TYP: Type annotations
CLN: Code cleanup
CHORE: Maintenance and other tasks that do not modify source or test files
```

#### CIPs

Chroma Improvement Proposals or CIPs (pronounced "Chips") are the way to propose new features or large changes to Chroma. If you plan to make a large change to Chroma please submit a CIP first so that the core Chroma team as well as the community can discuss the proposed change and provide feedback. A CIP should provide a concise technical specification of the feature and a rationale for why it is needed. The CIP should be submitted as a pull request to the [CIPs folder](https://github.com/chroma-core/chroma/tree/main/docs/cip). The CIP will be reviewed by the Chroma team and if approved will be merged into the repository. To learn more about writing a CIP you can read the [guide](https://github.com/chroma-core/chroma/blob/main/docs/cip/CIP_Chroma_Improvment_Proposals.md). CIPs are not required for small changes such as bug fixes or documentation updates.

A CIP starts in the "Proposed" state, then moves to "Under Review" once the Chroma team has reviewed it and is considering it for implementation. Once the CIP is approved it will move to the "Accepted" state and the implementation can begin. Once the implementation is complete the CIP will move to the "Implemented" state. If the CIP is not approved it will move to the "Rejected" state. If the CIP is withdrawn by the author it will move to the "Withdrawn" state.

#### Discord

For less fleshed out ideas you want to discuss with the community, you can join our [Discord](https://discord.gg/MMeYNTmh3x) and chat with us in the [#feature-ideas](https://discord.com/channels/1073293645303795742/1131592310786887700) channel. We are always happy to discuss new ideas and features with the community.


## Telemetry

Chroma contains a telemetry feature that collects **anonymous** usage information.

### Why?

We use this information to help us understand how Chroma is used, to help us prioritize work on new features and bug fixes, and to help us improve Chroma’s performance and stability.

### Opting out

If you prefer to opt out of telemetry, you can do this in two ways.

#### In Client Code

{% Tabs %}

{% Tab label="python" %}

Set `anonymized_telemetry` to `False` in your client's settings:

```python
from chromadb.config import Settings
client = chromadb.Client(Settings(anonymized_telemetry=False))
# or if using PersistentClient
client = chromadb.PersistentClient(path="/path/to/save/to", settings=Settings(anonymized_telemetry=False))
```

{% /Tab %}

{% Tab label="typescript" %}

Disable telemetry on your Chroma server (see next section) via environment variables where you run Chroma.

{% /Tab %}

{% /Tabs %}

#### In Chroma's Backend Server Using Environment Variables

Set `ANONYMIZED_TELEMETRY` to `False` in your shell or server environment.

If you are running Chroma on your local computer with `docker-compose` you can set this value in an `.env` file placed in the same directory as the `docker-compose.yml` file:

```
ANONYMIZED_TELEMETRY=False
```

### What do you track?

We will only track usage details that help us make product decisions, specifically:

- Chroma version and environment details (e.g. OS, Python version, is it running in a container, or in a jupyter notebook)
- Usage of embedding functions that ship with Chroma and aggregated usage of custom embeddings (we collect no information about the custom embeddings themselves)
- Client interactions with our hosted Chroma Cloud service.
- Collection commands. We track the anonymized uuid of a collection as well as the number of items
    - `add`
    - `update`
    - `query`
    - `get`
    - `delete`

We **do not** collect personally-identifiable or sensitive information, such as: usernames, hostnames, file names, environment variables, or hostnames of systems being tested.

To view the list of events we track, you may reference the **[code](https://github.com/chroma-core/chroma/blob/main/chromadb/telemetry/product/events.py)**

### Where is telemetry information stored?

We use **[Posthog](https://posthog.com/)** to store and visualize telemetry data.

{% Banner type="tip" %}

Posthog is an open source platform for product analytics. Learn more about Posthog on **[posthog.com](https://posthog.com/)** or **[github.com/posthog](https://github.com/posthog/posthog)**

{% /Banner %}
