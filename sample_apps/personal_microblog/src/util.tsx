import { Collection } from "chromadb";
import { TweetModel } from "./types";
import { customAlphabet } from 'nanoid'

export function chromaQueryResultsToPostModels(queryResult: any): TweetModel[] {
  if (queryResult.ids.length !== 1) {
    throw new Error("Expected 1 query, got " + queryResult.ids.length);
  }
  let getResult = {
    ids: queryResult.ids[0],
    documents: queryResult.documents[0],
    metadatas: queryResult.metadatas[0],
  }
  return chromaGetResultsToPostModels(getResult);
}

export function chromaGetResultsToPostModels(getResult: any): TweetModel[] {
  var postModels = getResult.ids.map(function (id: string, i: number) {
    return {
      id: id,
      threadParentId: getResult.metadatas[i]?.threadParentId,
      body: getResult.documents[i],
      date: getResult.metadatas[i]?.date,
      status: getResult.metadatas[i]?.status,
      role: getResult.metadatas[i]?.role,
      aiReplyId: getResult.metadatas[i]?.aiReplyId,
    };
  });
  return postModels;
}

export async function addPostModelToChromaCollection(
  post: TweetModel,
  collection: Collection
) {
  if (post.body.length === 0) {
    throw new Error("Post body is empty");
  }
  await collection.upsert({
    documents: [post.body],
    metadatas: [
      {
        threadParentId: post.threadParentId ?? "",
        date: post.date,
        status: post.status ?? '',
        role: post.role,
        aiReplyId: post.aiReplyId ?? "",
      },
    ],
    ids: [post.id],
  });
}

export function unixTimestampNow(): number {
  return Math.floor(Date.now() / 1000);
}

// https://zelark.github.io/nano-id-cc/ -- 1% chance of collision after 4M posts
const nanoid = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)

export function generateId(): string {
  return nanoid();
}
