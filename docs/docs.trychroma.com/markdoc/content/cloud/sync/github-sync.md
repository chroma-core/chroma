---
id: gh-sync
name: GitHub Sync
---

# Walkthrough

## Direct Sync

1. **Prerequisites**

    This walkthrough assumes that you have a GitHub account with at least one repository.

2. **Setup**

    If you do not already have a Chroma Cloud account, you will need to create one at https://trychroma.com. After creating an account, you can create a database by specifying a name:

    [INSERT SCREENSHOT]

    Once you have a database, you should create an API key to be able to access the Sync Function’s API. You can choose to make this API key scoped to all databases on your account or only the one you just created:

    [INSERT SCREENSHOT]

    The final setup step is to install the Chroma GitHub App into your GitHub account or organization. You can do this via the Chroma dashboard:

    [INSERT SCREENSHOT]

3. **Creating a source**

    To create a source, you can select a GitHub repository from the list of repositories that you made available to the Chroma GitHub App in the previous step.

    [INSERT SCREENSHOT]

4. **Invoking the Sync Function**

    To invoke the Sync Function, you must select a source on which to create the invocation. See the previous step for details on how to create a source. Once you select the source, you can invoke the Sync Function by clicking “[INSERT COPY]”:

    [INSERT SCREENSHOT]

## Platform Sync

1. **Prerequisites**

    This walkthrough assumes that you have an existing GitHub App that has been installed into at least one non-empty GitHub account or organization.

2. **Setup**

    If you do not already have a Chroma Cloud account, you will need to create one at https://trychroma.com. After creating an account, you can create a database by specifying a name:

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
