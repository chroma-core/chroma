import { Collection } from "chromadb";
import { TweetModel } from "./types";

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
  await collection.add({
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
