import { Collection } from "chromadb";
import { TweetModelBase } from "./types";
import { customAlphabet } from 'nanoid'

export function chromaQueryResultsToPostModels(queryResult: any): TweetModelBase[] {
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

export function chromaGetResultsToPostModels(getResult: any): TweetModelBase[] {
  var postModels = getResult.ids.map(function (id: string, i: number) {
    return {
      id: id,
      threadParentId: getResult.metadatas[i]?.threadParentId,
      body: getResult.documents[i],
      date: getResult.metadatas[i]?.date,
      status: getResult.metadatas[i]?.status,
      role: getResult.metadatas[i]?.role,
      aiReplyId: getResult.metadatas[i]?.aiReplyId,
      citations: splitCitations(getResult.metadatas[i]?.citations),
    };
  });
  return postModels;
}

export function splitCitations(citations: string): string[] {
  if (citations == undefined || citations.length == 0) {
    return [];
  }
  return citations.split(",");
}

export async function addPostModelToChromaCollection(
  post: TweetModelBase,
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
        citations: post.citations.join(","),
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

// https://zelark.github.io/nano-id-cc/ -- 1% chance of collision after 33K posts
const nanoid = customAlphabet('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 6)

export function generateId(): string {
  return nanoid();
}

export function formatDate(date: number): string {
  return new Date(date * 1000).toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
}

export function getReferencedPostsIds(post: TweetModelBase): string[] {
  const ids = [...post.citations];
  if (post.aiReplyId) {
    ids.push(post.aiReplyId);
  }
  return ids;
}
