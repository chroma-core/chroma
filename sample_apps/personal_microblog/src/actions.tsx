"use server";

import { TweetModel, TweetStatus } from "@/types";
import { ChromaClient } from "chromadb";
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";
import OpenAI from "openai";
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

const openAIClient = new OpenAI({ apiKey: OPENAI_API_KEY });

export async function getPosts(): Promise<TweetModel[]> {
  var posts = await collection.get({
    where: { role: "user" },
    include: ["documents", "metadatas"],
  });
  return chromaGetResultsToPostModels(posts).reverse();
}

const idToPost: { [id: string]: TweetModel } = {};
var assistantPosts = await collection.get({
  where: { role: "assistant" },
  include: ["documents", "metadatas"],
});
var assistantPostModels = chromaGetResultsToPostModels(assistantPosts);
assistantPostModels.forEach((post) => {
  idToPost[post.id] = post;
});

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

export async function getPostReplies(postId: string): Promise<TweetModel[]> {
  const replies = await collection.get({
    where: { threadParentId: postId },
    include: ["documents", "metadatas"],
  });
  return chromaGetResultsToPostModels(replies);
}

export async function semanticSearch(query: string): Promise<TweetModel[]> {
  const context = await collection.query({
    queryTexts: [query],
    nResults: 5,
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
    body: "",
    date: unixTimestampNow(),
    status: "processing",
  };
  addPostModelToChromaCollection(assistantPost, collection).catch(
    console.error
  );
  idToPost[id] = assistantPost;
  processAssistantResponse(userInput, assistantPost).catch(console.error); // Run in background
  return id;
}

async function processAssistantResponse(userInput: string, post: TweetModel) {
  const context = await collection.query({
    queryTexts: [userInput],
    nResults: 5,
  });
  const formattedContext = context.documents[0].map((doc, i) => {
    return `
    ${i + 1}. ${doc}
    `;
  });
  const contextString = formattedContext.join("\n");
  const prompt = `
  You are an AI assistant that helps users with their questions.
  ${contextString}
  1. Keep your responses short and to the point.
  2. Use markdown for formatting.
  User: ${userInput}
  Assistant:
  `;
  const response = await openAIClient.responses.create({
    model: "gpt-4.1",
    input: prompt,
    max_output_tokens: 240,
  });
  post.body = response.output_text;
  if (response.error) {
    post.status = "error";
  } else {
    post.status = "done";
  }
  addPostModelToChromaCollection(post, collection).catch(console.error);
}
