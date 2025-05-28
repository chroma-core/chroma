"use server";

import { PostModel, PostStatus, Role } from "@/types";
import { ChromaClient } from "chromadb";
import OpenAI from "openai";

const CHROMA_HOST = process.env.CHROMA_HOST;
const CHROMA_CLOUD_API_KEY = process.env.CHROMA_CLOUD_API_KEY;
const CHROMA_TENANT = process.env.CHROMA_TENANT;
const CHROMA_DB = process.env.CHROMA_DB;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

/*
const chromaClient = new ChromaClient({
  path: process.env.CHROMA_HOST,
  auth: {
    provider: "token",
    credentials: process.env.CHROMA_CLOUD_API_KEY,
    tokenHeaderType: "X_CHROMA_TOKEN"
  },
  tenant: process.env.CHROMA_TENANT,
  database: process.env.CHROMA_DB_NAME
});
const chromaCollection = await chromaClient.getCollection({ name: "posts" });
*/

const openAIClient = new OpenAI({ apiKey: OPENAI_API_KEY });

const posts: PostModel[] = [
  {
    id: "ai-resp",
    role: "assistant",
    body: "Hello, AI world!",
    date: new Date().toISOString(),
    status: "done",
  },
  {
    id: "1",
    role: "user",
    body: "Hello, world!",
    date: new Date().toISOString(),
    replyId: "ai-resp",
    status: "done",
  },
  {
    id: "2",
    role: "user",
    body: "# Markdown Header 1\n\n## Markdown Header 2\n\nMarkdown *emphasis* **strong** `code`\n\n---\nhorizontal rule\n\n---\n\n",
    date: new Date().toISOString(),
    status: "done",
  },
  {
    id: "4",
    role: "user",
    body: "Hello, <b>world!!</b>",
    date: new Date().toISOString(),
    status: "done",
  },
];

const idToPost: { [key: string]: PostModel } = {};
posts.forEach((post) => {
  idToPost[post.id] = post;
});

export async function getPosts(): Promise<PostModel[]> {
  await new Promise((resolve) => setTimeout(resolve, 200)); // Wait 2 seconds
  return posts.filter((post) => post.role === "user").toReversed();
}

export async function getPostById(id: string): Promise<PostModel | null> {
  /**
   * This function will poll for the post to finish processing.
   */
  const MAX_WAIT_MS = 10000; // e.g. wait max 10s
  const POLL_INTERVAL_MS = 200;

  const start = Date.now();
  let post = idToPost[id];

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
  posts.push(newPost);
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
  posts.push(assistantPost);
  idToPost[id] = assistantPost;
  processAssistantResponse(userInput, assistantPost).catch(console.error); // Run in background
  return id;
}

async function processAssistantResponse(userInput: string, post: PostModel) {
  const prompt = `
  You are an AI assistant that helps users with their questions.
  1. Keep your responses short and to the point.
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
}
