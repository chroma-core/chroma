"use server";

import { generateUUID, getChromaClient } from "@/lib/server-utils";
import { Chat } from "@/lib/models";

export const getChatsCollections = async () => {
  const client = await getChromaClient();
  return await client.getOrCreateCollection({
    name: "chats",
  });
};

export const getChats = async () => {
  const chats_collection = await getChatsCollections();
  const chats = await chats_collection.get();
  return chats.ids
    .map((id: string, i) => {
      return {
        id,
        title: chats.documents[i],
        created: chats.metadatas[i]?.created || new Date().toISOString(),
      } as Chat;
    })
    .sort((a, b) => Date.parse(b.created) - Date.parse(a.created));
};

export const createChat = async () => {
  const { id, timestamp } = await generateUUID();
  const chat: Chat = { id, title: "", created: timestamp };
  const chats_collection = await getChatsCollections();
  await chats_collection.add({
    ids: [chat.id],
    documents: [chat.title],
    metadatas: [{ created: chat.created }],
  });
  return chat;
};
