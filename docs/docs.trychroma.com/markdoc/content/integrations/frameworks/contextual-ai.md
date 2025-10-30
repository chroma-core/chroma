---
id: contextual-ai
name: Contextual AI
---

# Contextual AI

[Contextual AI](https://contextual.ai/?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo) provides enterprise-grade components for building production RAG agents. It offers state-of-the-art document parsing, reranking, generation, and evaluation capabilities that integrate seamlessly with Chroma as the vector database. Contextual AI's tools enable developers to build document intelligence applications with advanced parsing, instruction-following reranking, grounded generation with minimal hallucinations, and natural language testing for response quality.

![](https://img.shields.io/badge/License-Commercial-blue.svg)

| [Docs](https://docs.contextual.ai/user-guides/beginner-guide?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo) | [GitHub](https://github.com/ContextualAI?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo) | [Examples](https://github.com/ContextualAI/examples) | [Blog](https://contextual.ai/blog/?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo) |

You can use Chroma together with Contextual AI's Parse, Rerank, Generate, and LMUnit APIs to build and evaluate comprehensive RAG pipelines.

## Installation

```terminal
pip install chromadb contextual-client
```

### Complete RAG Pipeline

#### Parse documents and store in Chroma

{% Tabs %}
{% Tab label="python" %}

```python
from contextual import ContextualAI
import chromadb
from chromadb.utils import embedding_functions

# Initialize clients
contextual_client = ContextualAI(api_key=os.environ["CONTEXTUAL_AI_API_KEY"])
chroma_client = chromadb.EphemeralClient()

# Parse document
with open("document.pdf", "rb") as f:
    parse_response = contextual_client.parse.create(
        raw_file=f,
        parse_mode="standard",
        enable_document_hierarchy=True
    )

# Monitor job status (Parse API is asynchronous)
from time import sleep
while True:
    status = contextual_client.parse.job_status(parse_response.job_id)
    if status.status == "completed":
        break
    elif status.status == "failed":
        raise Exception("Parse job failed")
    sleep(30)  # Wait 30 seconds before checking again

# Get results after job completion
results = contextual_client.parse.job_results(
    parse_response.job_id,
    output_types=['blocks-per-page']
)

# Create Chroma collection
openai_ef = embedding_functions.OpenAIEmbeddingFunction(
    api_key=os.environ["OPENAI_API_KEY"],
    model_name="text-embedding-3-small"
)

# Create or get existing collection
collection = chroma_client.create_collection(
    name="documents",
    embedding_function=openai_ef,
    get_or_create=True
)

# Add parsed content to Chroma
texts, metadatas, ids = [], [], []

for page in results.pages:
    for block in page.blocks:
        if block.type in ['text', 'heading', 'table']:
            texts.append(block.markdown)
            metadatas.append({
                "page": page.index + 1,
                "block_type": block.type
            })
            ids.append(f"block_{block.id}")

collection.add(
    documents=texts,
    metadatas=metadatas,
    ids=ids
)
```

{% /Tab %}
{% Tab label="typescript" %}

```typescript
import ContextualAI, { toFile } from "contextual-client";
import { ChromaClient, OpenAIEmbeddingFunction } from "chromadb";
import fs from "node:fs";

const contextual = new ContextualAI({
  apiKey: process.env.CONTEXTUAL_AI_API_KEY!,
});
const chroma = new ChromaClient();
const embedder = new OpenAIEmbeddingFunction({
  apiKey: process.env.OPENAI_API_KEY!,
  model: "text-embedding-3-small",
});

const parseRes = await contextual.parse.create({
  raw_file: await toFile(fs.createReadStream("document.pdf"), "document.pdf", {
    type: "application/pdf",
  }),
  parse_mode: "standard",
  enable_document_hierarchy: true,
});

// Monitor job status (Parse API is asynchronous)
while (true) {
  const s = await contextual.parse.jobStatus(parseRes.job_id);
  if (s.status === "completed") break;
  if (s.status === "failed") throw new Error("Parse job failed");
  await new Promise((r) => setTimeout(r, 30000));
}

// Get results after job completion
const results = await contextual.parse.jobResults(parseRes.job_id, {
  output_types: ["blocks-per-page"],
});

// Create or get existing collection
const collection = await chroma.getOrCreateCollection({
  name: "documents",
  embeddingFunction: embedder,
});

// Add parsed content to Chroma
const texts: string[] = [];
const metadatas: Array<Record<string, string | number | boolean | null>> = [];
const ids: string[] = [];

for (const page of results.pages ?? []) {
  for (const block of page.blocks ?? []) {
    if (["text", "heading", "table"].includes(block.type)) {
      texts.push(block.markdown);
      metadatas.push({ page: (page.index ?? 0) + 1, block_type: block.type });
      ids.push(`block_${block.id}`);
    }
  }
}

await collection.add({ documents: texts, metadatas, ids });
```

> Note: If your Chroma JS package does not expose `OpenAIEmbeddingFunction`, define a small embedder using the OpenAI SDK instead:

```typescript
import OpenAI from "openai";
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const embedder = {
  generate: async (texts: string[]) => {
    const res = await openai.embeddings.create({
      model: "text-embedding-3-small",
      input: texts,
    });
    return res.data.map((d) => d.embedding);
  },
} as any;
```

{% /Tab %}
{% /Tabs %}

#### Query Chroma and rerank results with custom instructions

{% Tabs %}
{% Tab label="python" %}

```python
# Query Chroma
query = "What are the key findings?"
results = collection.query(
    query_texts=[query],
    n_results=10
)

# Rerank with instruction-following
rerank_response = contextual_client.rerank.create(
    query=query,
    documents=results['documents'][0],
    metadata=[str(m) for m in results['metadatas'][0]],
    model="ctxl-rerank-v2-instruct-multilingual",
    instruction="Prioritize recent documents. Technical details and specific findings should rank higher than general information."
)

# Get top documents
top_docs = [
    results['documents'][0][r.index]
    for r in rerank_response.results[:5]
]
```

{% /Tab %}
{% Tab label="typescript" %}

```typescript
const query = "What are the key findings?";
const q = await collection.query({ queryTexts: [query], nResults: 10 });
const docs: string[] = (q.documents?.[0] ?? []).filter(
  (d): d is string => typeof d === "string"
);

const rerankResponse = await contextual.rerank.create({
  query,
  documents: docs,
  metadata: (q.metadatas?.[0] ?? []).map((m) => JSON.stringify(m)),
  model: "ctxl-rerank-v2-instruct-multilingual",
  instruction:
    "Prioritize recent documents. Technical details and specific findings should rank higher than general information.",
});

const topDocsAll = rerankResponse.results
  .slice(0, 5)
  .map((r: { index: number }) => (q.documents?.[0] ?? [])[r.index]);
const topDocs: string[] = topDocsAll.filter(
  (d): d is string => typeof d === "string"
);
```

{% /Tab %}
{% /Tabs %}

#### Generate grounded response

{% Tabs %}
{% Tab label="python" %}

```python
# Generate grounded response
generate_response = contextual_client.generate.create(
    messages=[{
        "role": "user",
        "content": query
    }],
    knowledge=top_docs,
    model="v1",  # Supported models: v1, v2
    avoid_commentary=False,
    temperature=0.7
)

print("Response:", generate_response.response)
```

{% /Tab %}
{% Tab label="typescript" %}

```typescript
const generateResponse = await contextual.generate.create({
  messages: [{ role: "user", content: query }],
  knowledge: topDocs,
  model: "v1", // Supported models: v1, v2
  avoid_commentary: false,
  temperature: 0.7,
});

console.log("Response:", generateResponse.response);
```

{% /Tab %}
{% /Tabs %}

#### Evaluate response quality with LMUnit

{% Tabs %}
{% Tab label="python" %}

```python
# Evaluate generated response quality
lmunit_response = contextual_client.lmunit.create(
    query=query,
    response=generate_response.response,
    unit_test="The response should be technically accurate and cite specific findings"
)

print(f"Quality Score: {lmunit_response.score}")

# Score interpretation (continuous scale 1-5):
# 5 = Excellent - Fully satisfies criteria
# 4 = Good - Minor issues
# 3 = Acceptable - Some issues
# 2 = Poor - Significant issues
# 1 = Unacceptable - Fails criteria
```

{% /Tab %}
{% Tab label="typescript" %}

```typescript
const lmunitResponse = await contextual.lmUnit.create({
  query,
  response: generateResponse.response,
  unit_test:
    "The response should be technically accurate and cite specific findings",
});

console.log("Quality Score:", lmunitResponse.score);
// Score interpretation (continuous scale 1-5):
// 5 = Excellent - Fully satisfies criteria
// 4 = Good - Minor issues
// 3 = Acceptable - Some issues
// 2 = Poor - Significant issues
// 1 = Unacceptable - Fails criteria
```

{% /Tab %}
{% /Tabs %}

## Advanced Usage

For more advanced usage examples including table extraction, document hierarchy preservation, and multi-document RAG pipelines, please refer to the comprehensive examples in our Jupyter notebooks:

- [Contextual AI + Chroma Examples](https://github.com/ContextualAI/examples/tree/main/18-contextualai-chroma?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo)

## Components

### Parse API

Advanced document parsing that handles PDFs, DOCX, and PPTX files with:

- Document hierarchy preservation through parent-child relationships
- Intelligent table extraction with automatic splitting for large tables
- Multiple output formats: markdown-document, markdown-per-page, blocks-per-page
- Figure and caption extraction

[Parse API Documentation](https://docs.contextual.ai/api-reference/parse/parse-file?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo)

### Rerank API

State-of-the-art reranker with instruction-following capabilities:

- BEIR benchmark-leading accuracy
- Custom reranking instructions for domain-specific requirements
- Handles conflicting retrieval results
- Multi-lingual support

Models: `ctxl-rerank-v2-instruct-multilingual`, `ctxl-rerank-v2-instruct-multilingual-mini`, `ctxl-rerank-v1-instruct`

[Rerank API Documentation](https://docs.contextual.ai/api-reference/rerank/rerank?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo)

### Generate API (GLM)

Grounded Language Model optimized for minimal hallucinations:

- Industry-leading groundedness for RAG applications, currently #1 on the [FACTS Grounding benchmark](https://www.kaggle.com/benchmarks/google/facts-grounding) from Google DeepMind
- Knowledge attribution for source transparency
- Conversational context support
- Optimized for enterprise use cases

**Supported Models:** `v1`, `v2`

[Generate API Documentation](https://docs.contextual.ai/api-reference/generate/generate?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo)

### LMUnit API

Natural language unit testing for LLM response evaluation:

- State-of-the-art response quality assessment
- Structured testing methodology
- Domain-agnostic evaluation framework
- API-based evaluation at scale

**Scoring Scale (Continuous 1-5):**

- **5**: Excellent - Fully satisfies criteria
- **4**: Good - Minor issues
- **3**: Acceptable - Some issues
- **2**: Poor - Significant issues
- **1**: Unacceptable - Fails criteria

[LMUnit Documentation](https://docs.contextual.ai/api-reference/lmunit/lmunit?utm_campaign=Standalone-api-integration&utm_source=chroma&utm_medium=github&utm_content=repo)
