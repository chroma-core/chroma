import { PostModel } from "@/types";
import { ChromaClient } from "chromadb";

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

const posts: PostModel[] = [
  {
    id: "1",
    role: "user",
    body: "Hello, world!",
    date: new Date().toISOString(),
    reply: "I'm the AI response!",
  },
  {
    id: "2",
    role: "user",
    body: "Hello, world!",
    date: new Date().toISOString(),
  },
  {
    id: "4",
    role: "user",
    body: "Hello, world!",
    date: new Date().toISOString(),
  },
];

export async function getPosts(): Promise<PostModel[]> {
  await new Promise((resolve) => setTimeout(resolve, 200)); // Wait 2 seconds
  return posts;
}

export async function publishNewPost(newPost: PostModel) {
  posts.push(newPost);

}
