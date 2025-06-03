"use server";

import { TweetModel, TweetStatus } from "@/types";
import { ChromaClient } from "chromadb";
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";
import { openai } from '@ai-sdk/openai';
import { generateText, generateObject, jsonSchema } from 'ai';
import {
  addPostModelToChromaCollection,
  chromaGetResultsToPostModels,
  chromaQueryResultsToPostModels,
  unixTimestampNow,
} from "./util";

const CHROMA_HOST = process.env.CHROMA_HOST;
const CHROMA_CLOUD_API_KEY = process.env.CHROMA_CLOUD_API_KEY;
const CHROMA_TENANT = process.env.CHROMA_TENANT;
const CHROMA_DB = process.env.CHROMA_DB;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const usingChromaLocal = CHROMA_HOST === "localhost";

if (CHROMA_HOST == null) {
  throw new Error("CHROMA_HOST is not set");
}
if (CHROMA_HOST == 'api.trychroma.com' && CHROMA_CLOUD_API_KEY == null) {
  throw new Error("CHROMA_CLOUD_API_KEY is not set");
}
if (CHROMA_TENANT == null) {
  throw new Error("CHROMA_TENANT is not set");
}
if (CHROMA_DB == null) {
  throw new Error("CHROMA_DB is not set");
}
if (OPENAI_API_KEY == null) {
  throw new Error("OPENAI_API_KEY is not set");
}
const llmModel = openai('gpt-4-turbo')

const client = new ChromaClient({
  ssl: !usingChromaLocal,
  host: CHROMA_HOST,
  database: CHROMA_DB,
  tenant: CHROMA_TENANT,
  headers: {
    "x-chroma-token": CHROMA_CLOUD_API_KEY ?? "",
  },
});

const collection = await client.getOrCreateCollection({
  name: "personal-blog-tweets",
  embeddingFunction: new OpenAIEmbeddingFunction({
    apiKey: OPENAI_API_KEY ?? "",
    modelName: "text-embedding-3-small",
  }),
});

export async function getPosts(page: number): Promise<TweetModel[]> {
  if (page == null || page < 0 || isNaN(page)) {
    page = 0;
  }
  const posts = await collection.get({
    where: { role: "user" },
    include: ["documents", "metadatas"],
  });
  const postModels = chromaGetResultsToPostModels(posts);
  const pageSize = 15;
  const documents = postModels.length;
  let start = documents - (page + 1) * pageSize;
  if (start < 0) {
    start = 0;
  }
  const end = documents - page * pageSize;
  if (end < 0) {
    return [];
  }
  return postModels
    .slice(start, end)
    .reverse();
}

export async function getPostById(id: string): Promise<TweetModel | null> {
  const chromaResult = await collection.get({
    ids: [id],
    include: ["documents", "metadatas"],
  });

  if (chromaResult.documents.length === 1) {
    return chromaGetResultsToPostModels(chromaResult)[0];
  } else {
    return null;
  }
}

export async function getPostReplies(postId: string): Promise<{
  aiReplies: TweetModel[],
  userReplies: TweetModel[],
}> {
  const replies = await collection.get({
    where: { threadParentId: postId },
    include: ["documents", "metadatas"],
  });
  const aiReplies = chromaGetResultsToPostModels(replies).filter((post) => post.role === "assistant");
  const userReplies = chromaGetResultsToPostModels(replies).filter((post) => post.role === "user");
  return { aiReplies, userReplies };
}

export async function semanticSearch(query: string): Promise<TweetModel[]> {
  const context = await collection.query({
    queryTexts: [query],
    nResults: 5,
    where: {
      "role": "user",
    }
  });
  return chromaQueryResultsToPostModels(context);
}

export async function publishNewUserPost(newPostBody: string, threadParentId?: string): Promise<TweetModel> {
  const newPost: TweetModel = {
    id: crypto.randomUUID(),
    threadParentId: threadParentId,
    role: "user",
    body: newPostBody,
    date: unixTimestampNow(),
    status: "done",
  };
  if (newPost.body.includes("@assistant")) {
    const assistanceResponseId = getAssistantReponse(newPostBody, newPost.id);
    newPost.aiReplyId = assistanceResponseId;
  }
  addPostModelToChromaCollection(newPost, collection).catch(console.error);
  return newPost;
}

function getAssistantReponse(userInput: string, parentThreadId: string): string {
  const id = crypto.randomUUID();
  const assistantPost: TweetModel = {
    id,
    threadParentId: parentThreadId,
    role: "assistant",
    body: "Thinking...",
    date: unixTimestampNow(),
    status: "processing",
  };
  addPostModelToChromaCollection(assistantPost, collection).catch(
    console.error
  );
  processAssistantResponse(userInput, assistantPost).catch(console.error); // Run in background
  return id;
}

async function processAssistantResponse(userInput: string, post: TweetModel) {
  const cleanedUserInput = userInput.replace(/@assistant/g, "").trim();
  try {
    const [semanticContext, queryRange] = await Promise.all([
      collection.query({
        queryTexts: [cleanedUserInput],
        nResults: 5,
        where: {
          "role": "user",
        }
      }),
      generateQueryRange(userInput),
    ]);
    let context = chromaQueryResultsToPostModels(semanticContext);

    if (false && queryRange.start != null && queryRange.end != null) {
      const temporalContext = await collection.query({
        queryTexts: [cleanedUserInput],
        nResults: 5,
        where: {
          "$and": [
            {
              "date": {
                "$gte": queryRange.start,
              },
            },
            {
              "date": {
                "$lte": queryRange.end,
              },
            }
          ],
        },
      });
      context.push(...chromaQueryResultsToPostModels(temporalContext));
      context = context.filter((post, index, self) =>
        index === self.findIndex((t) => t.id === post.id)
      );
    }

    const formattedContext = context.map((post, i) => {
      return `${new Date(post.date * 1000).toISOString()}: ${post.body}`;
    }).join("\n\n");

    const { text } = await generateText({
      model: llmModel,
      messages: [
        {
          role: 'system',
          content: `You are an AI assistant that helps users with their questions.

          Context from previous posts:
          ${formattedContext}

          Instructions:
          1. Keep your responses short and to the point.
          2. Use markdown for formatting and don't use emojis.
          3. Base your response on the context provided when relevant.
          4. If the context does not provide enough information, tell the user that you don't know.`
        },
        {
          role: 'user',
          content: userInput
        }
      ],
      maxTokens: 240,
    });

    post.body = text;
    post.status = "done";
  } catch (error) {
    console.error('Error generating assistant response:', error);
    post.body = "Sorry, I encountered an error while processing your request.";
    post.status = "error";
  }

  addPostModelToChromaCollection(post, collection).catch(console.error);
}

interface QueryRange {
  /* UNIX timestamp */
  start?: number;
  end?: number;
}

async function generateQueryRange(userInput: string): Promise<QueryRange> {
  const { object } = await generateObject({
    model: llmModel,
    schema: jsonSchema<QueryRange>({
      type: "object",
      properties: {
        start: { type: "string", format: "date-time" },
        end: { type: "string", format: "date-time" },
      },
    }),
    messages: [
      {
        role: 'system',
        content: `You will determine the appropriate time range that should be searched based on the user's query.
        The current date is ${new Date().toISOString()}.
        The user's query is: ${userInput}
        The time range should be in the format of this json:
        {
          "start": "YYYY-MM-DD HH:MM:SS",
          "end": "YYYY-MM-DD HH:MM:SS"
        }
        If the user's query is not related to time, return {}.

        Examples:
        Current date: 2025-06-01
        User: "What did I do last week?"
        Response: {
          "start": "2025-05-25 00:00:00",
          "end": "2025-06-01 00:00:00"
        }
        User: "What did I do last month?"
        Response: {
          "start": "2025-05-01 00:00:00",
          "end": "2025-06-01 00:00:00"
        }
        User: "How do I make cookies?"
        Response: {}
        `
      },
      {
        role: 'user',
        content: userInput
      }
    ],
  });
  return {
    start: object.start ? new Date(object.start).getTime() / 1000 : undefined,
    end: object.end ? new Date(object.end).getTime() / 1000 : undefined,
  };
}
