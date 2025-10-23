---
id: gh-sync
name: GitHub Sync
---

# Walkthrough

## Direct Sync

Direct Sync is the default syncing method, which uses the Chroma Cloud GitHub app. To use your own custom GitHub app, use [Platform Sync](/cloud/sync/overview#platform-sync).

1. **Prerequisites**

    This walkthrough assumes that you have a GitHub account with at least one repository.

2. **New database setup**

    If you do not already have a Chroma Cloud account, you will need to create one at [trychroma.com](https://www.trychroma.com). After creating an account, you can create a database by specifying a name:

  {% MarkdocImage lightSrc="/sync/sync_create_database.png" darkSrc="sync/sync_create_database.png" alt="Create database screen" /%}

    On the setup screen, select "Sync a GitHub repo":

    {% MarkdocImage lightSrc="/sync/sync_new_db_onboarding.png" darkSrc="sync/sync_create_database.png" alt="Onboarding screen for syncing a GitHub repo" /%}

    Install the Chroma GitHub App into your GitHub account or organization:

    {% MarkdocImage lightSrc="/sync/sync_new_db_github_install.png" darkSrc="sync/sync_new_db_github_install.png" alt="GitHub app installation screen" /%}

    And follow the prompts to initiate sync. Choose the **repo** to sync code from, the **branch or commit hash** version of the code to index, and new **collection name** for the synced code. (The collection will be created by the syncing process, and must not exist yet.)

    {% MarkdocImage lightSrc="/sync/sync_install.png" darkSrc="sync/sync_install.png" alt="Sync repo to Chroma Collection UI" /%}


3. **Existing database setup**

    Open an existing database in Chroma Cloud, and select "Sync" from the menu:

    {% MarkdocImage lightSrc="/sync/sync_existing_db.png" darkSrc="/sync/sync_existing_db.png" alt="Sync tab in Chroma Cloud UI" /%}


    On the Sync page, select "Create" to begin syncing code. If you have not already connected GitHub, you may be prompted to install the Chroma Cloud GitHub app again.

    {% MarkdocImage lightSrc="/sync/sync_existing_db_add.png" darkSrc="/sync/sync_existing_db_add.png" alt="Create path for a new Sync" /%}

    Then, follow the prompts to initiate sync. Choose the **repo** to sync code from, the **branch or commit hash** version of the code to index, and new **collection name** for the synced code. (The collection will be created by the syncing process, and must not exist yet.)

    {% MarkdocImage lightSrc="/sync/sync_existing_db_sync.png" darkSrc="/sync/sync_existing_db_sync.png" alt="Create flow for a new Sync" /%}


4. **Viewing an Invocation**

    Each Sync create a new Invocation. When completed, select "View Collection" to see the new Chroma collection containing the synced code:

    {% MarkdocImage lightSrc="/sync/db_invocation.png" darkSrc="/sync/db_invocation.png" alt="Invocation screen for a Sync" /%}


## Platform Sync

1. **Prerequisites**

    This walkthrough assumes that you have an existing GitHub App that has been installed into at least one non-empty GitHub account or organization.

2. **Setup**

    If you do not already have a Chroma Cloud account, you will need to create one at [trychroma.com](https://www.trychroma.com). After creating an account, you can create a database by specifying a name:

    [INSERT SCREENSHOT]

    Once you have a database, you should create an API key to be able to access the Sync Function’s API. You can choose to make this API key scoped to all databases on your account or only the one you just created:

    [INSERT SCREENSHOT]

    The final setup step is to grant Chroma access to the repositories to which your GitHub App has access. You will need to retrieve the app’s ID and private key from GitHub:

    [INSERT SCREENSHOT]

    With these credentials, you can use the Chroma dashboard to register your GitHub App with Chroma:

    [INSERT SCREENSHOT]

3. **Creating a source**

    To create a source, you should select the GitHub App you registered with Chroma in the previous step:

    [INSERT SCREENSHOT]

    The dashboard will display the repositories available to this GitHub App, and from this list you can select the repository for which you would like to create a source:

    [INSERT SCREENSHOT]

    Alternatively, you can create a source by sending an API request to the Sync Function’s API:

    ```bash
    curl -X POST https://sync.trychroma.com/api/v1/sources \
        -H "x-chroma-token: <YOUR_CHROMA_API_KEY>" \
        -H "Content-Type: application/json" \
        -d '{
            "database_name": "<YOUR_DATABASE_NAME>",
            "embedding_model": "Qwen/Qwen3-Embedding-0.6B",
            "github": {
            "repository": "chroma-core/chroma",
            "app_id": "<YOUR_GITHUB_APP_ID>"
            }
        }'
    ```

4. **Invoking the Sync Function**

    To invoke the Sync Function, you must select a source on which to create the invocation. See the previous step for details on how to create a source. Once you select the source, you can invoke the Sync Function by clicking “[INSERT COPY]”:

    [INSERT SCREENSHOT]

    Alternatively, you can invoke the Sync Function by sending an API request to the Sync Function’s API:

    ```bash
    curl -X POST https://sync.trychroma.com/api/v1/sources/{source_id}/invocations \
        -H "x-chroma-token: <YOUR_CHROMA_API_KEY>" \
        -H "Content-Type: application/json" \
        -d '{
            "target_collection_name": "<YOUR_TARGET_COLLECTION_NAME>",
            "ref_identifier": {
                    // only one of these should be supplied
                    "branch": "<YOUR_BRANCH_NAME>",
                    "sha": "<YOUR_COMMIT_SHA>"
                }
        }'
    ```
