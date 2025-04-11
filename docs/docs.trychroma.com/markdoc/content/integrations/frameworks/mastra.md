---
id: mastra
name: Mastra
---

# Building a RAG Application with ChromaDB and Mastra

Welcome! This guide demonstrates how to leverage ChromaDB as a powerful vector store within a Retrieval-Augmented Generation (RAG) application built using the Mastra framework. We'll also use Cohere for generating embeddings.

{% Banner type="info" %}
**What is Mastra?** Mastra is an open-source TypeScript agent framework designed to provide the essential primitives for building AI applications. It enables developers to create AI agents with memory and tool-calling capabilities, implement deterministic LLM workflows, and leverage RAG for knowledge integration. With features like model routing, workflow graphs, and automated evals, Mastra provides a complete toolkit for developing, testing, and deploying AI applications.
{% /Banner %}

{% Banner type="tip" %}
**Code Example:** You can find the complete code for the application built in this guide on GitHub: [akuya-ekorot/chroma-mastra](https://github.com/akuya-ekorot/chroma-mastra)
{% /Banner %}

This example showcases the seamless integration between ChromaDB and Mastra, highlighting how developers can easily build sophisticated AI applications by combining these technologies.

## Goal

The primary goal of this guide is to walk you through the process of using ChromaDB effectively within a Mastra-based RAG pipeline. We will cover:

1.  Setting up a Mastra project configured to use ChromaDB.
2.  Creating custom API endpoints in Mastra to interact with ChromaDB.
3.  Processing text documents: chunking them and generating embeddings using Cohere.
4.  Storing these embeddings and associated metadata efficiently in your ChromaDB instance using Mastra's ChromaDB integration (`@mastra/chroma`).
5.  Querying ChromaDB through Mastra to retrieve relevant document chunks based on semantic similarity to a user's query.

By the end of this guide, you will have a functional Mastra application demonstrating a core RAG workflow, centered around ChromaDB for vector storage and retrieval.

Let's get started!

---

## Step 1: Setting Up ChromaDB

Before we start building the Mastra application, we need a running ChromaDB instance to store our vector embeddings.

**1. Install ChromaDB:**

The simplest way to get ChromaDB is by installing it using pip (Python's package installer). This package includes both the client library and a server component. Open your terminal and run:

```bash
pip install chromadb
```

**2. Run the ChromaDB Server:**

Navigate to the directory where you want ChromaDB to store its data (or stay in your project directory) and run the server:

```bash
chroma run --path ./chroma
```

This command starts the ChromaDB server and tells it to store data in a `./chroma` subdirectory. By default, the server listens on `http://localhost:8000`.

Keep this terminal window open; the ChromaDB server needs to be running in the background while you develop and run the Mastra application.

---

## Step 2: Setting Up Your Mastra Project

Now, let's create the Mastra application structure. We'll use the `create-mastra` CLI tool, which scaffolds a new project for us.

**1. Create the Mastra Project:**

Open a *new* terminal window (keep the ChromaDB server running in the other one) and run the following command:

```bash
npx create-mastra@latest
```

This command initiates an interactive setup process. You'll be asked for:

*   **Project Name:** Choose a name for your project (e.g., `mastra-chroma-rag`).
*   **Components to Install:** You'll see options like `Agents`, `Tools`, and `Workflows`. **For this guide, do not select any of these optional components.** Press Enter to continue without selecting any. We will be creating custom API routes later instead of using standard agents or tools.
*   **Default Provider:** Select an LLM provider (like OpenAI, Anthropic, etc.). While we focus on ChromaDB and Cohere embeddings, the base Mastra setup requires a default provider.
*   **Include Example Code:** You can choose "No" as we'll be adding our own specific code.
*   **IDE Integration (MCP Server):** Choose based on your preference.

The CLI will then create the project directory, install dependencies, and set up the basic configuration.

**2. Configure API Keys:**

Navigate into your newly created project directory:

```bash
cd your-project-name # Replace with the name you chose
```

Mastra uses a `.env` file for managing API keys. Open the `.env` file created by the setup process and add the necessary API keys. At a minimum, you'll need the key for the LLM provider you selected during setup. **Crucially, you also need to add your Cohere API key now**, as we will use it for generating embeddings.

Example `.env` content (replace with your actual keys):

```dotenv
# Example for OpenAI (or your chosen default provider)
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# Add your Cohere API Key here
COHERE_API_KEY=your_cohere_api_key
```

For more detailed information on setting up Mastra, refer to the [official installation documentation](https://mastra.ai/docs/getting-started/installation).

---

## Step 3: Install Additional Dependencies

Our RAG application requires specific packages for handling embeddings, interacting with ChromaDB, processing documents, and utilizing AI SDK utilities.

Navigate to your Mastra project directory in the terminal (the one created in Step 2) and run the following command to install the necessary dependencies:

```bash
npm install @ai-sdk/cohere @mastra/chroma @mastra/rag hono ai
```

*(If you are using pnpm or yarn, use `pnpm add ...` or `yarn add ...` respectively.)*

Here's why we need these packages:

*   `@ai-sdk/cohere`: Provides the integration for using Cohere's embedding models (like `embed-english-v3.0` used in our routes) via the AI SDK. We'll use this to convert text chunks and queries into vectors.
*   `@mastra/chroma`: This is Mastra's dedicated package for interacting with ChromaDB. It provides the `ChromaVector` class (used in `src/mastra/store/index.ts`) to connect to, create indexes in, and perform operations (like `upsert` and `query`) on your ChromaDB instance.
*   `@mastra/rag`: Contains helpful utilities for building RAG applications within Mastra. We'll use `MDocument` from this package to represent our text data and its `chunk` method (as seen in `src/mastra/routes/index.ts`) to split the text into manageable pieces before embedding.
*   `hono`: Mastra uses Hono internally for its API server. We install it directly to access Hono's types (like `Context`) for type safety when defining our custom API route handlers in `src/mastra/routes/index.ts`.
*   `ai`: The core AI SDK package. It provides essential functions like `embed` (for single text embedding) and `embedMany` (for batch embedding), which we use in our routes to generate the vector representations needed for ChromaDB.

---

## Step 4: Configure the ChromaDB Vector Store

With the necessary packages installed, we need to configure our Mastra application to connect to the running ChromaDB server. We'll do this by creating a dedicated file to initialize the ChromaDB client provided by `@mastra/chroma`.

**1. Create the Store File:**

In your Mastra project, create a new directory `src/mastra/store` if it doesn't exist, and inside it, create a file named `index.ts`.

```bash
mkdir -p src/mastra/store
touch src/mastra/store/index.ts
```

**2. Add ChromaDB Connection Code:**

Open `src/mastra/store/index.ts` and add the following code:

```typescript
import { ChromaVector } from '@mastra/chroma';

// Define the ChromaDB connection URL (ensure this is correct for your setup)
// It defaults to the standard ChromaDB port 8000 on localhost.
// You can override this by setting the CHROMA_PATH environment variable.
const chromaPath = process.env.CHROMA_PATH || 'http://localhost:8000';

console.log(`Connecting to ChromaDB at: ${chromaPath}`);

// Instantiate and export the ChromaVector instance
// This instance will be used by our API routes to interact with ChromaDB.
export const chroma = new ChromaVector({ path: chromaPath });

// Optional: Log existing indexes on startup for debugging or confirmation.
// This helps verify the connection is successful.
chroma.listIndexes().then((indexes) => {
  console.log('Successfully connected to ChromaDB. Existing indexes:', indexes);
}).catch(error => {
  console.error('Failed to connect to ChromaDB or list indexes:', error);
  console.error(`Ensure ChromaDB is running at ${chromaPath} (started in Step 1).`);
});

```

**Explanation:**

*   We import `ChromaVector` from the `@mastra/chroma` package installed in the previous step.
*   We define `chromaPath`, which holds the URL of your ChromaDB server. It defaults to `http://localhost:8000`, the default for ChromaDB.
    *   **Important:** Make sure this URL matches the address where your ChromaDB server (started in Step 1) is actually running. If you configured ChromaDB to use a different host or port, update the default value here or set the `CHROMA_PATH` environment variable.
*   We create an instance of `ChromaVector`, passing the `path`. This `chroma` object will be our interface for all ChromaDB operations (creating indexes, adding vectors, querying, etc.).
*   We export the `chroma` instance so it can be imported and used in other parts of our application, like the API routes we'll create next.
*   The optional logging at the end attempts to list existing indexes, providing immediate feedback in the console on whether the connection was successful when the Mastra server starts.

---

## Step 5: Creating Custom API Routes

Instead of using Mastra's standard Agent or Workflow components, we'll create custom API endpoints to handle our RAG logic directly. Mastra makes this easy by allowing you to register custom routes that leverage the underlying Hono server framework. This gives you fine-grained control over the request/response cycle.

For more details on server customization options like middleware and CORS, see the [Mastra Server Documentation](https://mastra.ai/docs/deployment/server#custom-api-routes).

Let's create the routes needed for our ChromaDB RAG application.

**1. Create the Routes File:**

In your Mastra project, create a new directory `src/mastra/routes` if it doesn't exist, and inside it, create a file named `index.ts`.

```bash
mkdir -p src/mastra/routes
touch src/mastra/routes/index.ts
```

**2. Add Route Logic:**

Open `src/mastra/routes/index.ts` and add the following code. We'll break down what each part does below.

```typescript
import { registerApiRoute } from "@mastra/core/server";
import type { Context } from 'hono';
import { MDocument } from "@mastra/rag"; // Import MDocument
import { embed, embedMany } from "ai"; // Import embed, embedMany and Embedding type
import { cohere } from "@ai-sdk/cohere"; // Import cohere
import { chroma } from '../store'; // Import the shared chroma instance
import { randomUUID } from 'node:crypto'; // For generating unique IDs

// Define constants for configuration
const COHERE_EMBEDDING_MODEL = 'embed-english-v3.0'; // Cohere model for embeddings
const CHROMA_INDEX_NAME = 'documents'; // Name for our ChromaDB index
const DEFAULT_QUERY_TOP_K = 5; // Default number of results for query endpoint

/**
 * Route 1: Process and Embed Text (/process-embed)
 * - Method: POST
 * - Purpose: Receives text content, chunks it, generates embeddings using Cohere,
 *            and stores the chunks and embeddings in ChromaDB.
 * - Input: JSON body { "content": "your text here" }
 * - Output: JSON response indicating success or failure.
 */
const processAndEmbedRoute = registerApiRoute('/process-embed', {
  method: 'POST',
  handler: async (c: Context) => {
    let body: { content?: string };

    // 1. Parse and Validate Request Body
    try {
      body = await c.req.json();
      if (!body || typeof body.content !== 'string' || body.content.trim() === '') {
        return c.json({ error: 'Request body must be JSON with a non-empty "content" string.' }, 400);
      }
    } catch (error) {
      return c.json({ error: 'Invalid JSON format.' }, 400);
    }
    const textContent = body.content;
    console.log(`Received content for processing: ${textContent.substring(0, 100)}...`);

    try {
      // 2. Create MDocument and Chunk
      console.log("Creating MDocument and chunking...");
      const doc = MDocument.fromText(textContent);
      const chunks = await doc.chunk({ strategy: "recursive", size: 256, overlap: 50 });
      console.log(`Document chunked into ${chunks.length} pieces.`);
      if (chunks.length === 0) return c.json({ message: 'Content resulted in zero chunks.' });

      // 3. Generate Embeddings (using Cohere via AI SDK)
      console.log(`Generating embeddings with ${COHERE_EMBEDDING_MODEL}...`);
      const { embeddings } = await embedMany({
        model: cohere.embedding(COHERE_EMBEDDING_MODEL),
        values: chunks.map(chunk => chunk.text),
      });
      console.log(`Generated ${embeddings.length} embeddings.`);
      if (embeddings.length !== chunks.length) throw new Error("Chunk and embedding count mismatch.");

      // 4. Prepare Data for ChromaDB (IDs, Embeddings, Metadata)
      const ids = chunks.map(chunk => (chunk.metadata?.id || randomUUID()).toString());
      const metadatas = chunks.map(chunk => ({
        ...chunk.metadata,
        text: chunk.text, // Store original text in metadata
        source: 'api-upload', // Example metadata field
      }));

      // 5. Ensure ChromaDB Index Exists
      console.log(`Checking/Creating Chroma index '${CHROMA_INDEX_NAME}'...`);
      const existingIndexes = await chroma.listIndexes();
      if (!existingIndexes.includes(CHROMA_INDEX_NAME)) {
        console.log(`Creating index '${CHROMA_INDEX_NAME}'...`);
        // Cohere v3 English model dimension is 1024
        await chroma.createIndex({ indexName: CHROMA_INDEX_NAME, dimension: 1024, metric: 'cosine' });
        console.log(`Index '${CHROMA_INDEX_NAME}' created.`);
      }

      // 6. Upsert Data into ChromaDB
      console.log(`Upserting ${embeddings.length} vectors into Chroma index: ${CHROMA_INDEX_NAME}...`);
      await chroma.upsert(CHROMA_INDEX_NAME, embeddings, metadatas, ids);
      console.log("Successfully upserted vectors.");

      // 7. Return Success
      return c.json({
        success: true,
        message: `Processed content, generated ${embeddings.length} embeddings, stored in '${CHROMA_INDEX_NAME}'.`,
        chunksProcessed: chunks.length,
      });

    } catch (error: any) {
      console.error("Error during processing/embedding:", error);
      return c.json({ error: 'Failed to process document.', details: error.message }, 500);
    }
  }
});

/**
 * Route 2: Query Documents (/query-documents)
 * - Method: GET
 * - Purpose: Receives a query text, generates its embedding using Cohere,
 *            and queries ChromaDB for similar document chunks.
 * - Input: URL query parameters `?query=your search text` (required) & `&topK=3` (optional)
 * - Output: JSON response with query details and ranked results from ChromaDB.
 */
const queryDocumentsRoute = registerApiRoute('/query-documents', {
  method: 'GET',
  handler: async (c: Context) => {
    // 1. Get and Validate Query Parameters
    const queryText = c.req.query('query');
    if (!queryText) return c.json({ error: "Missing 'query' parameter." }, 400);

    const topKParam = c.req.query('topK');
    let topK = DEFAULT_QUERY_TOP_K;
    if (topKParam) {
      const parsed = parseInt(topKParam, 10);
      if (!isNaN(parsed) && parsed > 0) topK = parsed;
      else return c.json({ error: "'topK' must be a positive integer." }, 400);
    }
    console.log(`Received query: "${queryText}", topK: ${topK}`);

    try {
      // 2. Generate Query Embedding (using Cohere via AI SDK)
      console.log(`Generating embedding for query with ${COHERE_EMBEDDING_MODEL}...`);
      const { embedding: queryVector } = await embed({
        model: cohere.embedding(COHERE_EMBEDDING_MODEL),
        value: queryText,
      });

      // 3. Query ChromaDB
      console.log(`Querying Chroma index '${CHROMA_INDEX_NAME}'...`);
      const results = await chroma.query({
        indexName: CHROMA_INDEX_NAME,
        queryVector: queryVector,
        topK: topK,
        // includeVector: false, // Default is false
      });
      console.log(`Retrieved ${results.length} results.`);

      // 4. Return Results
      return c.json({ query: queryText, topK: topK, results: results });

    } catch (error: any) {
      console.error(`Error during query processing for "${queryText}":`, error);
      // Handle index not found specifically
      if (error.message?.toLowerCase().includes('index') && error.message?.toLowerCase().includes('does not exist')) {
        return c.json({ error: `Index '${CHROMA_INDEX_NAME}' not found. Process documents first.` }, 404);
      }
      return c.json({ error: 'Failed to query vector store.', details: error.message }, 500);
    }
  }
});

/**
 * Export an array containing all custom API routes.
 * This array will be imported and used in the main Mastra configuration file.
 */
export const customApiRoutes = [
  processAndEmbedRoute,
  queryDocumentsRoute,
];

```

**Explanation:**

*   **Imports:** We bring in necessary functions and types:
    *   `registerApiRoute` from Mastra to define our endpoints.
    *   `Context` from `hono` for type hints in our route handlers.
    *   `MDocument` from `@mastra/rag` for easy text processing and chunking.
    *   `embed` and `embedMany` from `ai` (AI SDK) for generating embeddings.
    *   `cohere` from `@ai-sdk/cohere` to specify the Cohere embedding model.
    *   Our `chroma` instance from `../store` (created in Step 4) to interact with ChromaDB.
    *   `randomUUID` for generating unique IDs for chunks if needed.
*   **Constants:** We define constants for the Cohere model name, the ChromaDB index name, and the default number of search results (`topK`) for better maintainability.
*   **`/process-embed` Route:**
    *   Uses `registerApiRoute` to define a `POST` endpoint at `/process-embed`.
    *   The `handler` function receives the Hono context `c`.
    *   It parses the incoming JSON body, expecting a `content` field.
    *   It creates an `MDocument` from the text, then uses `.chunk()` to split it based on size and overlap.
    *   It calls `embedMany` with the Cohere model and the text chunks to get vector embeddings.
    *   It prepares arrays of IDs, embeddings, and metadata (including the original text chunk) for ChromaDB.
    *   It checks if the target ChromaDB index exists using `chroma.listIndexes()` and creates it using `chroma.createIndex()` if necessary, specifying the dimension (1024 for Cohere v3 English) and metric ('cosine').
    *   It uses `chroma.upsert()` to add or update the data in ChromaDB.
    *   It returns a JSON response indicating success or failure.
*   **`/query-documents` Route:**
    *   Uses `registerApiRoute` to define a `GET` endpoint at `/query-documents`.
    *   It reads the `query` and optional `topK` parameters from the URL.
    *   It calls `embed` with the Cohere model and the query text to get a single query vector.
    *   It uses `chroma.query()` with the index name, query vector, and `topK` to find the most similar documents in ChromaDB.
    *   It returns a JSON response containing the original query and the results (which include metadata and similarity scores).
*   **`customApiRoutes` Export:** We export an array containing all the defined routes. This array will be imported and registered in our main Mastra configuration file in the next step.

---

## Step 6: Registering Custom Routes

The final step in our setup is to tell our Mastra application about the custom API routes we just created. We do this in the main Mastra configuration file, `src/mastra/index.ts`.

**1. Edit the Mastra Configuration:**

Open the file `src/mastra/index.ts` in your editor.

**2. Register the Routes:**

Modify the file to import the `customApiRoutes` array we exported from `src/mastra/routes/index.ts` and pass it to the `Mastra` constructor within the `server.apiRoutes` configuration option.

Replace the contents of `src/mastra/index.ts` with the following:

```typescript
import { Mastra } from '@mastra/core';
import { customApiRoutes } from './routes'; // Import the custom routes we defined

// Instantiate the main Mastra application
export const mastra = new Mastra({
  // Server configuration is where we register custom routes
  server: {
    apiRoutes: customApiRoutes // Pass the imported routes array here
  }
  // Other Mastra configurations (like agents, workflows, tools, logger, etc.)
  // can be added here if needed for other functionalities.
  // For this specific guide focusing on custom routes and ChromaDB,
  // we only need to configure the server routes.
});

```

**Explanation:**

*   We import `Mastra` from `@mastra/core`.
*   We import the `customApiRoutes` array that we exported from `./routes/index.ts` in the previous step.
*   We instantiate `Mastra`. Inside the configuration object, we add a `server` key.
*   Within the `server` object, we set the `apiRoutes` property to our imported `customApiRoutes` array.
*   This tells Mastra to make the `/process-embed` and `/query-documents` endpoints available when the server runs.
*   We removed the explicit `Logger` configuration for simplicity, but added comments indicating where other configurations like agents, workflows, or logging could be added if the application were expanded.

With this final piece in place, our Mastra application is configured to use ChromaDB via the custom API routes.

---

## Step 7: Running and Testing the Application

Now that everything is set up, let's run the Mastra server and test our custom endpoints.

**1. Prerequisites Check:**

*   **ChromaDB Server:** Ensure the ChromaDB server you started in Step 1 is still running in its terminal window. You should see logs indicating it's listening (usually on port 8000).
*   **Cohere API Key:** Double-check that you have added your `COHERE_API_KEY` to the `.env` file in the root of your Mastra project directory (as described in Step 2).

**2. Start the Mastra Server:**

Navigate to your Mastra project directory in your terminal (the one containing `package.json`, `src`, etc.). Run the development server using the script defined in `package.json`:

```bash
npm run dev
```

*(Use `pnpm dev`, `yarn dev`, or `bun dev` if you used a different package manager).*

You should see output indicating the Mastra server is starting up. It will likely log the connection attempt to ChromaDB (from Step 4) and then indicate that it's listening on a specific port, typically `http://localhost:4111`.

**3. Test the `/process-embed` Endpoint:**

This endpoint takes text content, chunks it, embeds it using Cohere, and stores it in ChromaDB. Open a *new* terminal window (keep the Mastra server and ChromaDB server running) and use `curl` to send a POST request.

Replace `"Mastra is a cool framework..."` with any text you want to add to the vector store.

```bash
curl -X POST http://localhost:4111/process-embed \
-H "Content-Type: application/json" \
-d '{
  "content": "Mastra is a cool framework for building AI applications. It integrates well with vector stores like ChromaDB. ChromaDB is a vector database designed for AI applications. Cohere provides powerful embedding models."
}'
```

**Expected Response:** You should receive a JSON response indicating success, along with the number of chunks processed. Example:

```json
{
  "success": true,
  "message": "Processed content, generated 4 embeddings, stored in 'documents'.",
  "chunksProcessed": 4
}
```

You should also see log messages in the Mastra server terminal showing the processing steps (chunking, embedding, upserting).

Feel free to run this command multiple times with different text content to populate your ChromaDB index.

**4. Test the `/query-documents` Endpoint:**

This endpoint takes a query string, embeds it, and searches ChromaDB for the most similar text chunks previously added. Use `curl` to send a GET request.

Replace `"What is Mastra?"` with your search query.

```bash
# Basic query (uses default topK=5)
curl -G http://localhost:4111/query-documents --data-urlencode "query=What is Mastra?"

# Query with specific topK
curl -G http://localhost:4111/query-documents --data-urlencode "query=Tell me about ChromaDB" --data-urlencode "topK=2"
```

**Expected Response:** You'll get a JSON response containing your original query, the `topK` value used, and an array of `results`. Each result object in the array represents a relevant chunk found in ChromaDB and includes:

*   `id`: The unique ID of the chunk.
*   `metadata`: The metadata stored with the chunk (including the original `text`).
*   `score`: A similarity score (closer to 1 means more similar, based on the 'cosine' metric).

Example (truncated):

```json
{
  "query": "What is Mastra?",
  "topK": 5,
  "results": [
    {
      "id": "...",
      "metadata": {
        "text": "Mastra is a cool framework for building AI applications. It integrates well with vector stores like ChromaDB.",
        "source": "api-upload",
        ...
      },
      "score": 0.85...
    },
    {
      "id": "...",
      "metadata": {
        "text": "ChromaDB is a vector database designed for AI applications. Cohere provides powerful embedding models.",
        "source": "api-upload",
        ...
      },
      "score": 0.62...
    }
    // ... potentially more results up to topK
  ]
}
```

If you query before adding any documents, you'll receive an error indicating the index wasn't found.

---

Congratulations! You have successfully set up a Mastra application that uses custom API routes to interact with ChromaDB for a basic RAG workflow, leveraging Cohere embeddings. You can now build upon this foundation to create more complex AI applications.

---

## Resources

- **Mastra Documentation:** [https://mastra.ai/docs](https://mastra.ai/docs)
- **Mastra GitHub Repository:** [https://github.com/mastra-ai/mastra](https://github.com/mastra-ai/mastra)
- **`@mastra/chroma` Package:** [https://www.npmjs.com/package/@mastra/chroma](https://www.npmjs.com/package/@mastra/chroma)
- **Example Code for this Guide:** [https://github.com/akuya-ekorot/chroma-mastra](https://github.com/akuya-ekorot/chroma-mastra)
