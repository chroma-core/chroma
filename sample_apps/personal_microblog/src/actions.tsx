"use server";

import { PartialAssistantPost, TweetModel, TweetStatus } from "@/types";
import { openai } from '@ai-sdk/openai';
import { createStreamableValue, StreamableValue } from 'ai/rsc';
import { generateText, generateObject, jsonSchema, streamText } from 'ai';
import {
  addPostModelToChromaCollection,
  chromaGetResultsToPostModels,
  chromaQueryResultsToPostModels,
  generateId,
  unixTimestampNow,
} from "./util";


import { ChromaClient } from "chromadb";
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";

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

const chromaClient = new ChromaClient({
  ssl: !usingChromaLocal,
  host: CHROMA_HOST,
  database: CHROMA_DB,
  tenant: CHROMA_TENANT,
  headers: {
    "x-chroma-token": CHROMA_CLOUD_API_KEY ?? "",
  },
});

const chromaCollection = await chromaClient.getOrCreateCollection({
  name: "personal-blog-tweets",
  embeddingFunction: new OpenAIEmbeddingFunction({
    apiKey: OPENAI_API_KEY ?? "",
    modelName: "text-embedding-3-small",
  }),
});

const llmModel = openai('gpt-4-turbo')

/**
 * Returns the posts from `cursor` to `cursor - pageSize`, inclusive
 * The new cursor is `cursor - pageSize - 1`
 */
export async function getPosts(cursor?: number): Promise<{ posts: TweetModel[], cursor: number }> {
  if (cursor != undefined && cursor <= -1) {
    return {
      posts: [],
      cursor: -1,
    };
  }
  const pageSize = 15;
  if (cursor != undefined) {
    let start = cursor - pageSize + 1;
    if (start < 0) {
      start = 0;
    }
    const posts = await chromaCollection.get({
      where: { "role": "user" },
      include: ["documents", "metadatas"],
      limit: cursor - start,
      offset: start,
    });
    const postModels = chromaGetResultsToPostModels(posts);
    return {
      posts: postModels.reverse(),
      cursor: start - 1,
    };
  } else {
    const posts = await chromaCollection.get({
      where: { "role": "user" },
      include: ["documents", "metadatas"],
    });
    const postModels = chromaGetResultsToPostModels(posts);
    const count = postModels.length;
    let start = count - pageSize + 1;
    if (start < 0) {
      start = 0;
    }
    return {
      posts: postModels.slice(start).reverse(),
      cursor: start - 1,
    };
  }
}

export async function getPostById(id: string): Promise<TweetModel | undefined> {
  const post = await chromaCollection.get({
    ids: [id],
  });
  const res = chromaGetResultsToPostModels(post)
  return res.length > 0 ? res[0] : undefined;
}

export async function getPostReplies(id: string): Promise<TweetModel[]> {
  const posts = await chromaCollection.get({
    where: {
      "threadParentId": id,
    }
  });
  return chromaGetResultsToPostModels(posts);
}

export async function semanticSearch(query: string): Promise<TweetModel[]> {
  const posts = await chromaCollection.query({
    queryTexts: [query],
    nResults: 5,
  });
  return chromaQueryResultsToPostModels(posts);
}

export async function publishNewUserPost(newPostBody: string, threadParentId?: string): Promise<{ userPost: TweetModel, assistantPost: PartialAssistantPost | undefined }> {
  const newPost: TweetModel = {
    id: generateId(),
    threadParentId: threadParentId,
    role: "user",
    body: newPostBody,
    date: unixTimestampNow(),
    status: "done",
  };
  let partialAssistantPost: PartialAssistantPost | undefined;
  if (newPost.body.includes("@assistant")) {
    partialAssistantPost = getAssistantReponse(newPostBody, newPost.id);
    newPost.aiReplyId = partialAssistantPost?.id;
  }
  addPostModelToChromaCollection(newPost, chromaCollection).catch(console.error);
  return { userPost: newPost, assistantPost: partialAssistantPost };
}

function getAssistantReponse(userInput: string, parentThreadId: string): PartialAssistantPost {
  const id = generateId();
  const stream = createStreamableValue<string, any>();
  const assistantPost: PartialAssistantPost = {
    id,
    threadParentId: parentThreadId,
    role: "assistant",
    body: "Thinking...",
    date: unixTimestampNow(),
    status: "processing",
    stream: stream.value,
  };
  addPostModelToChromaCollection(assistantPost, chromaCollection).catch(console.error);
  processAssistantResponse(userInput, assistantPost, stream).catch(console.error); // Run in background
  return assistantPost;
}

async function processAssistantResponse(userInput: string, post: PartialAssistantPost, stream: any) {
  const cleanedUserInput = userInput.replace(/@assistant/g, "").trim();
  try {
    const [semanticContext, queryRange] = await Promise.all([
      chromaCollection.query({
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
      const temporalContext = await chromaCollection.query({
        queryTexts: [cleanedUserInput],
        nResults: 5,
        where: {
          "$and": [
            {
              "date": {
                "$gte": queryRange.start as number,
              },
            },
            {
              "date": {
                "$lte": queryRange.end as number,
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

    const { textStream } = await streamText({
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

    let text = "";
    for await (const textPart of textStream) {
      text += textPart;
      stream.update(text);
    }

    post.body = text;
    post.status = "done";
  } catch (error) {
    console.error('Error generating assistant response:', error);
    post.body = "Sorry, I encountered an error while processing your request.";
    post.status = "error";
  } finally {
    stream.done();
  }

  addPostModelToChromaCollection(post, chromaCollection).catch(console.error);
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
