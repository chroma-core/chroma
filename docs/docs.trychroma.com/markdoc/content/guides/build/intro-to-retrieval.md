---
id: intro-to-retrieval
name: Intro to Retrieval
---

# Introduction to Retrieval

Large language models like GPT-5, Claude, or Gemini can write code, answer questions, generate content, and solve complex problems with remarkable sophistication. You can build chatbots, content generators, coding assistants, and analysis tools by crafting good prompts and calling AI APIs.

However, as you start to build more sophisticated applications, especially those that require knowledge not available to an LLM, you'll quickly run into some fundamental limitations.

## The Knowledge Problem

Imagine you're building an AI assistant for a company's customer support team. You want the AI to answer questions about your products, policies, and procedures.

Given a simple prompt

> What is your return policy for tooth brushes?

The AI is likely to return a generic answer, because it doesn't know about your company's specific policies:

> I'm not a retailer or company that sells toothbrushes - I'm Claude, an AI assistant created by Anthropic. I don't have products or a return policy. If you're looking for information about a toothbrush return policy, you'd need to check with the specific store or company where you purchased or are planning to purchase the toothbrush. Most retailers have their return policies listed on their websites or you can contact their customer service directly.

We can get around this problem, by providing useful information or context for the AI. This often called "context engineering":

> What is your return policy for tooth brushes? Use the following information for context: Tooth brushes can be returned up to 360 days after purchase if unopened. Tooth brushes are unable to be returned if they are used. Tooth brushes can be returned if they suffer manufacturing defects discovered after opening.

Now the AI has the information it needs to answer the user question correctly:

> Based on the information provided, our return policy for toothbrushes allows returns up to 360 days after purchase, as long as the toothbrush remains unopened and in its original packaging. This gives you nearly a full year to return an unopened toothbrush if needed. Once a toothbrush has been opened, it wouid not be eligible for return under this policy unless it has manufacturing defects.

But what happens if we want our AI chatbot to be able to answer questions about shipping, product specifications, troubleshooting, or any other topic? We will have to include our entire knowledge base in our context, which leads to several technical problems.

**Token Limits:** AI models have maximum input lengths. Even the largest models might not be able to fit an entire company's documentation in a single prompt.

**Cost:** AI APIs typically charge per token. Including thousands of words of context in every request becomes expensive quickly.

**Relevance:** When you include too much information, the AI can get confused or focus on irrelevant details instead of what actually matters for answering the user's question.

**Freshness:** Information changes constantly. Product specs update, policies change, new documentation gets written. Keeping everything in prompts means manually updating every prompt whenever anything changes.

**Hallucinations:** Without the correct information or focus for answering a user's question, LLMs may produce a wrong answer with an authoritative voice. For most business applications, where accuracy matters, hallucination is a critical problem.

## Enter Retrieval

Retrieval solves these fundamental challenges by creating a bridge between AI models and your actual data. Instead of trying to cram everything into prompts, a retrieval system **stores your information** in a searchable format. This allows you to search your knowledge base using natural language, so you can find relevant information to answer the user's question, by providing the retrieval system with the user's question itself. This way, you can build context for the model in a strategic manner.

When a retrieval system returns the results from your knowledge base relevant to the user's question, you can use them to provide context for the AI model to help it generate an accurate response.

Here's how a typical retrieval pipeline is built:

1. **Converting information into searchable formats** - this is done by using **embedding models**. They create mathematical representations of your data, called "embeddings", that capture the semantic meaning of text, not just keywords.
2. **Storing these representations** in a retrieval system, optimized for quickly finding similar embeddings for an input query.
3. **Processing user queries** into embeddings, so they can be used as inputs to your retrieval system.
4. **Query and retrieve** results from the database.
5. **Combining the retrieved results** with the original user query to serve to an AI model.

**Chroma** is a powerful retrieval system that handles most of this process out-of-the-box. It also allows you to customize these steps to get the best performance in your AI application. Let's see it in action for our customer support example.

### Step 1: Embed our Knowledge Base and Store it in a Chroma Collection

{% Tabs %}

{% Tab label="python" %}

Install Chroma:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="pip" %}

```terminal
pip install chromadb
```

{% /Tab %}

{% Tab label="poetry" %}

```terminal
poetry add chromadb
```

{% /Tab %}

{% Tab label="uv" %}

```terminal
uv pip install chromadb
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Chroma embeds and stores information in a single operation.

```python
import chromadb

client = chromadb.Client()
customer_support_collection = client.create_collection(
    name="customer support"
)

customer_support_collection.add(
   ids=["1", "2", "3"],
   documents=[
      "Toothbrushes can be returned up to 360 days after purchase if unopened.",
      "Shipping is free of charge for all orders.",
      "Shipping normally takes 2-3 business days"
   ]
)
```

{% /Tab %}

{% Tab label="typescript" %}

Install Chroma:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="npm" %}

```terminal
npm install chromadb @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="pnpm" %}

```terminal
pnpm add chromadb @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="yarn" %}

```terminal
yarn add chromadb @chroma-core/default-embed
```

{% /Tab %}

{% Tab label="bun" %}

```terminal
bun add chromadb @chroma-core/default-embed
```

{% /Tab %}

{% /TabbedUseCaseCodeBlock %}

Run a Chroma server locally:

```terminal
chroma run
```

Chroma embeds and stores information in a single operation.

```typescript
import { ChromaClient } from "chromadb";

const client = new ChromaClient();
const customer_support_collection = await client.createCollection({
  name: "customer support",
});

await customer_support_collection.add({
  ids: ["1", "2", "3"],
  documents: [
    "Toothbrushes can be returned up to 360 days after purchase if unopened.",
    "Shipping is free of charge for all orders.",
    "Shipping normally takes 2-3 business days",
  ],
});
```

{% /Tab %}

{% /Tabs %}

### Step 2: Process the User's Query

Similarly, Chroma handles the embedding of queries for you out-of-the-box.

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
user_query = "What is your return policy for tooth brushes?"

context = customer_support_collection.query(
    queryTexts=[user_query],
    n_results=1
)['documents'][0]

print(context) # Toothbrushes can be returned up to 360 days after purchase if unopened.
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
const user_query = "What is your return policy for tooth brushes?";

const context = (
  await customer_support_collection.query({
    queryTexts: [user_query],
    n_results: 1,
  })
).documents[0];

console.log(context); // Toothbrushes can be returned up to 360 days after purchase if unopened.
```

{% /Tab %}

{% /TabbedCodeBlock %}

### Step 3: Generate the AI Response

With the result from Chroma, we can build the correct context for an AI model.

{% CustomTabs %}

{% Tab label="OpenAI" %}

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
import os
import openai

openai.api_key = os.getenv("OPENAI_API_KEY")

prompt = f"{user_query}. Use this as context for answering: {context}"

response = openai.ChatCompletion.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": "You are a helpful assistant"},
        {"role": "user", "content": prompt}
    ]
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import OpenAI from "openai";

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

const prompt = `${userQuery}. Use this as context for answering: ${context}`;

const response = await openai.chat.completions.create({
  model: "gpt-4o",
  messages: [
    { role: "system", content: "You are a helpful assistant" },
    { role: "user", content: prompt },
  ],
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

{% /Tab %}

{% Tab label="Anthropic" %}

{% TabbedCodeBlock %}

{% Tab label="python" %}

```python
import os
import anthropic

client = anthropic.Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY")
)

prompt = f"{user_query}. Use this as context for answering: {context}"

response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1024,
    messages=[
        {"role": "user", "content": prompt}
    ]
)
```

{% /Tab %}

{% Tab label="typescript" %}

```typescript
import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY,
});

const prompt = `${userQuery}. Use this as context for answering: ${context}`;

const response = await client.messages.create({
  model: "claude-sonnet-4-20250514",
  max_tokens: 1024,
  messages: [
    {
      role: "user",
      content: prompt,
    },
  ],
});
```

{% /Tab %}

{% /TabbedCodeBlock %}

{% /Tab %}

{% /CustomTabs %}

There's a lot left to consider, but the core building blocks are here. Some next steps to consider:

- **Embedding Model** There are many embedding models on the market, some optimized for code, others for english and others still for various languages. Embedding model selection plays a big role in retrieval accuracy.
- **Chunking** Chunking strategies are very unique to the data. Deciding how large or small to make chunks is critical to the performance of the system.
- **n_results** varying the number of results balances token usage with correctness. The more results, the likely the better answer from the LLM but at the expense of more token usage.

