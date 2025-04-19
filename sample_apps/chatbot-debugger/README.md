# Chatbot Debugger

This sample app demonstrates how you can build a simple chatbot application with your own data using Chroma.

When the user submits a question, the app finds relevant data from its Chroma DB, and sends it to an LLM to produce an answer. This method is commonly known as RAG.

Beyond using Chroma for retrieval, we can also use it to debug conversations in the chatbot. Every message is persisted in a 'telemetry' collection. This way, you can use semantic search and other querying techniques in your Chroma DB, to see what similar questions users have, what questions have no good answers, and more!

To get started, download this sample app using the Chroma CLI:
```shell
chroma install chatbot-debugger
```
You will get the files for this app, as well as the Chroma DB powering it.

## Walkthrough

### Data

This app is configured to work with a Chroma DB with the following collections:
- `chroma-docs-data` - docs and code data from the Chroma documentation and code base.
- `chroma-docs-summaries` - short summaries of the data chunks from `chroma-docs-data`. These are used to help users understand what data was retrieved to answer their question.
- `chats` - persists all information about the different chats users started in the app.
- `telemetry` - saves all user and assistant messages.
- `retrieved-chunks` - persists a list of document IDs from `chroma-docs-data` that were used to produce an LLM answer.

The app relies on these collections, and the metadata defined on records in them to work properly.

The collection names are defined in constants in `lib/utils.ts`. If you choose to change them or make a collection with your own data, make sure to update these constants.

### Scripts

We provide two simple scripts to help you modify this app with your own data. You can find them in the `scripts` directory.
- `ingest` - is the code we used to generate the `-data` and `-summaries` collections. You can provide it a path on your local machine to get the same collections using your own files. Note that for simplicity we process file by file. You can get much better performance by processing data chunks in batches. For example, here is the command we ran to generate the Chroma DB over the Chroma docs and code from our open-source repo:

```shell
npm run ingest -- --collection chroma-docs --root ./chroma --extensions .md .py --directories docs/docs.trychroma.com chromadb/utils/embedding_functions
```
- `copy-to-cloud` - if you change the environment variables to match your Chroma Cloud connection credentials, you can use this script to copy your local Chroma DB to you Chroma Cloud account. This should make it much easier to debug conversations and experiment with your data on the Dashboard. For example, if you did not customize the host, tenant, and DB of your local Chroma server (most users don't), after setting your Cloud credentials in the environment variables, use the `chroma run` command to start your local Chroma server, and simply run:
```shell
npm run copy-to-cloud
```

