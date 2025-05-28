"use server";

import { PostModel, PostStatus, Role } from "@/types";
import { ChromaClient } from "chromadb";
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";
import OpenAI from "openai";
import {
  addPostModelToChromaCollection,
  chromaResultsToPostModels,
  makeTweetEntry,
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
  name: "test-collection",
  embeddingFunction: new OpenAIEmbeddingFunction({
    apiKey: OPENAI_API_KEY ?? "",
    modelName: "text-embedding-ada-002",
  }),
});

if ((await collection.count()) == 0) {
  const introTweet = makeTweetEntry(
    "assistant",
    "Hey! I'm your personal assistant! If you ever need my help remembering something, just mention me with @assistant"
  );
  addPostModelToChromaCollection(introTweet, collection).catch(console.error);
}

const openAIClient = new OpenAI({ apiKey: OPENAI_API_KEY });

export async function getPosts(): Promise<PostModel[]> {
  var posts = await collection.get({
    where: { role: "user" },
    include: ["documents", "metadatas"],
  });
  return chromaResultsToPostModels(posts).reverse();
}

const idToPost: { [id: string]: PostModel } = {};
var assistantPosts = await collection.get({
  where: { role: "assistant" },
  include: ["documents", "metadatas"],
});
var assistantPostModels = chromaResultsToPostModels(assistantPosts);
assistantPostModels.forEach((post) => {
  idToPost[post.id] = post;
});

export async function getPostById(id: string): Promise<PostModel | null> {
  /**
   * This function will poll for the post to finish processing.
   */
  const MAX_WAIT_MS = 10000; // e.g. wait max 10s
  const POLL_INTERVAL_MS = 200;

  const start = Date.now();

  var post = idToPost[id];
  while (Date.now() - start < MAX_WAIT_MS) {
    post = idToPost[id];
    if (!post) return null;

    if (post.status !== "processing") {
      return post;
    }

    await new Promise((res) => setTimeout(res, POLL_INTERVAL_MS));
  }

  post.status = "error";
  return post;
}

export async function publishNewPost(newPostBody: string): Promise<PostModel> {
  const newPost: PostModel = {
    id: crypto.randomUUID(),
    role: "user",
    body: newPostBody,
    date: new Date().toISOString(),
    status: "done",
  };
  if (newPost.body.includes("@assistant")) {
    const assistanceResponseId = getAssistantReponse(newPostBody);
    newPost.replyId = assistanceResponseId;
  }
  addPostModelToChromaCollection(newPost, collection).catch(console.error);
  return newPost;
}

function getAssistantReponse(userInput: string): string {
  const id = crypto.randomUUID();
  const assistantPost = {
    id,
    role: "assistant" as Role,
    body: "",
    date: new Date().toISOString(),
    status: "processing" as PostStatus,
  };
  addPostModelToChromaCollection(assistantPost, collection).catch(
    console.error
  );
  idToPost[id] = assistantPost;
  processAssistantResponse(userInput, assistantPost).catch(console.error); // Run in background
  return id;
}

async function processAssistantResponse(userInput: string, post: PostModel) {
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
  });
  post.body = response.output_text;
  if (response.error) {
    post.status = "error";
  } else {
    post.status = "done";
  }
  addPostModelToChromaCollection(post, collection).catch(console.error);
}
