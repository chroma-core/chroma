"use server";

import {
  Chat,
  chatMappingConfig,
  Chunk,
  chunkMappingConfig,
  chunkValidator,
  Message,
  messageMappingConfig,
  messageValidator,
} from "@/lib/types";
import {
  CHATS_COLLECTION,
  DATA_COLLECTION,
  getAppParams,
  recordsToObject,
  TELEMETRY_COLLECTION,
} from "@/lib/utils";
import { ChromaClient } from "chromadb";
import { v4 as uuidv4 } from "uuid";

export const getChromaClient = async () => {
  return new ChromaClient({ ...getAppParams().chromaClientParams });
};

export const getChromaCollection = async (
  client: ChromaClient,
  collectionName: string,
) => {
  try {
    return await client.getOrCreateCollection({
      name: collectionName,
    });
  } catch {
    throw new Error(
      `Failed to get the ${collectionName} collection. Verify that your Chroma server is running and that the '${collectionName}' collection exists.`,
    );
  }
};

export const getChats = async () => {
  const client = await getChromaClient();
  const chatsCollection = await getChromaCollection(client, CHATS_COLLECTION);

  let records;
  try {
    records = await chatsCollection.get();
  } catch {
    throw new Error("Failed to get chats");
  }

  return recordsToObject<Chat>(
    records,
    chatMappingConfig,
    "Some records in the 'chats' collection do not match the shape required by this app",
  ).reverse();
};

export const addTelemetry = async (input: {
  role: "user" | "assistant";
  content: string;
  chatId: string;
  chunks?: Chunk[];
}) => {
  const client = await getChromaClient();
  const telemetryCollection = await getChromaCollection(
    client,
    TELEMETRY_COLLECTION,
  );

  const baseMetadata = {
    role: input.role,
    timestamp: new Date().toISOString(),
    chat_id: input.chatId,
  };

  let metadata;
  if (input.chunks) {
    metadata = {
      ...baseMetadata,
      chunks: input.chunks.map((c) => c.id).join(", "),
    };
  } else {
    metadata = { ...baseMetadata };
  }

  const message: Message = {
    ...metadata,
    id: uuidv4(),
    content: input.content,
    chunks: input.chunks,
  };

  try {
    await telemetryCollection.add({
      ids: [message.id],
      documents: [message.content],
      metadatas: [metadata],
    });
    return message;
  } catch {
    throw new Error("Failed to add telemetry records");
  }
};

export const addChatRecord = async (firstMessage: string) => {
  const client = await getChromaClient();
  const chatsCollection = await getChromaCollection(client, CHATS_COLLECTION);

  const chat: Chat = {
    id: uuidv4(),
    title: "New Chat",
    created: new Date().toISOString(),
  };

  try {
    await chatsCollection.add({
      ids: [chat.id],
      documents: [chat.title],
      metadatas: [{ created: chat.created }],
    });
    await addTelemetry({
      role: "user",
      content: firstMessage,
      chatId: chat.id,
    });
    return chat;
  } catch {
    throw new Error(
      `Failed to add a new chat. Verify that you are able to add data to your 'chats' and 'telemetry' collections.`,
    );
  }
};

export const getChunks = async (chunkIds: string[]) => {
  const client = await getChromaClient();
  const dataCollection = await getChromaCollection(client, DATA_COLLECTION);

  let records;
  try {
    records = await dataCollection.get({
      ids: chunkIds,
    });
  } catch {
    throw new Error("Failed to get chunks");
  }

  return recordsToObject<Chunk>(
    records,
    chunkMappingConfig,
    "Some records in the data collection were corrupted. Please make sure they contain all the required fields for the app",
    chunkValidator,
  );
};

export const getChatMessages = async (chatId: string) => {
  const client = await getChromaClient();
  const telemetryCollection = await getChromaCollection(
    client,
    TELEMETRY_COLLECTION,
  );

  let records;
  try {
    records = await telemetryCollection.get({
      where: { chat_id: chatId },
    });
  } catch {
    throw new Error("Failed to get chats messages");
  }

  const messages = recordsToObject<Message>(
    records,
    messageMappingConfig,
    `Some messages for chat ${chatId} were corrupted. Please make sure they contain all the required fields for the app`,
    messageValidator,
  );

  if (messages.length === 0) {
    throw new Error(`No chat with ID ${chatId} was found`);
  }

  const chunkIds = messages.reduce((chunkIds, message) => {
    if (message.chunkIds) {
      const ids = message.chunkIds.split(",").map((chunkId) => chunkId.trim());
      return [...chunkIds, ...ids];
    }
    return chunkIds;
  }, [] as string[]);

  const chunks = (await getChunks(chunkIds)).reduce(
    (chunks, chunk) => {
      chunks[chunk.id] = chunk;
      return chunks;
    },
    {} as Record<string, Chunk>,
  );

  return messages.map((m) => {
    const chunkIds = m.chunkIds?.split(",").map((cid) => cid.trim());
    return {
      ...m,
      chunks: chunkIds?.map((cid) => chunks[cid] as Chunk) || undefined,
    } as Message;
  });
};

export const appSetup = async () => {
  const client = await getChromaClient();
  await client.heartbeat();
};
