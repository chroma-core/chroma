"use server";

import {
  AppError,
  Chat,
  chatMappingConfig,
  Chunk,
  chunkMappingConfig,
  Message,
  messageMappingConfig,
  MetadataValue,
  NewMessageRequest,
  Result,
  Role,
} from "@/lib/types";
import {
  getAppParams,
  recordsToObject,
} from "@/lib/utils";
import { ChromaClient, Collection } from "chromadb";
import { v4 as uuidv4 } from "uuid";
import { embed } from "@/lib/retrieval";
import {
  CHATS_COLLECTION, DATA_COLLECTION,
  RETRIEVED_CHUNKS_COLLECTION,
  SUMMARIES_COLLECTION,
  TELEMETRY_COLLECTION
} from "@/lib/constants";

let chromaClient: ChromaClient;

export const getChromaClient = async (): Promise<
  Result<ChromaClient, AppError>
> => {
  if (!chromaClient) {
    const appParamsResult = getAppParams();
    if (!appParamsResult.ok) {
      return appParamsResult;
    }
    chromaClient = new ChromaClient({
      ...appParamsResult.value.chromaClientParams,
      fetchOptions: {
        keepalive: true,
        cache: "force-cache",
      },
    });
  }
  return {
    ok: true,
    value: chromaClient,
  };
};

export const getChromaCollection = async (
  client: ChromaClient,
  name: string,
): Promise<Result<Collection, AppError>> => {
  try {
    const collection = await client.getOrCreateCollection({ name });
    return { ok: true, value: collection };
  } catch {
    return {
      ok: false,
      error: new Error(
        `Failed to get the ${name} collection. Verify that your Chroma server is running and that the '${name}' collection exists.`,
      ),
    };
  }
};

export const getCollections = async (
  client: ChromaClient,
  names: string[],
): Promise<Result<Record<string, Collection>, AppError>> => {
  const collections: Record<string, Collection> = {};
  for (const name of names) {
    const collectionResult = await getChromaCollection(client, name);
    if (!collectionResult.ok) {
      return collectionResult;
    }
    collections[name] = collectionResult.value;
  }
  return { ok: true, value: collections };
};

export const getChats = async (
  chatIds?: string[],
): Promise<Result<Chat[], AppError>> => {
  const clientResult = await getChromaClient();

  if (!clientResult.ok) {
    return clientResult;
  }

  const chatsCollectionResult = await getChromaCollection(
    clientResult.value,
    CHATS_COLLECTION,
  );

  if (!chatsCollectionResult.ok) {
    return chatsCollectionResult;
  }

  try {
    const records = await chatsCollectionResult.value.get(
      chatIds ? { where: { chat_id: { $in: chatIds } } } : {},
    );
    const chatsResult = recordsToObject<Chat>(
      records,
      chatMappingConfig,
      "Some records in the 'chats' collection do not match the shape required by this app",
    );

    if (!chatsResult.ok) {
      return chatsResult;
    }

    return { ok: true, value: chatsResult.value.reverse() };
  } catch {
    return {
      ok: false,
      error: new AppError("Failed to get records from the 'chats' collection"),
    };
  }
};

/**
 * We record every message in our chat-app in Chroma collections, that will allow us to both
 * persist data that we want to display, and our app's performance using Chroma's search capabilities.
 * For example, for every AI (assistant) response, we record what chunks were retrieved for it as context.
 * This can allow us to see which user questions got bad responses from the assistant, and how our own
 * data influenced the quality of the response.
 * @param input All the components making up a message in our application: chatId, content, role, and chunks.
 */
export const addTelemetry = async (
  input: NewMessageRequest,
): Promise<Result<Message, AppError>> => {
  const clientResult = await getChromaClient();
  if (!clientResult.ok) {
    return clientResult;
  }

  const collectionsResult = await getCollections(clientResult.value, [
    TELEMETRY_COLLECTION,
    RETRIEVED_CHUNKS_COLLECTION,
  ]);

  if (!collectionsResult.ok) {
    return collectionsResult;
  }

  const telemetryCollection = collectionsResult.value[TELEMETRY_COLLECTION];
  const retrievedChunksCollection =
    collectionsResult.value[RETRIEVED_CHUNKS_COLLECTION];

  const metadata: { [key: string]: MetadataValue } = {
    role: input.role,
    timestamp: new Date().toISOString(),
    chat_id: input.chatId || "",
  };

  const message: Message = {
    id: uuidv4(),
    content: input.content,
    role: metadata.role as Role,
    timestamp: metadata.timestamp as string,
    chat_id: metadata.chat_id as string,
    chunks: input.chunks,
  };

  const messageEmbedding = await embed([message.content]);
  if (!messageEmbedding.ok) {
    return messageEmbedding;
  }

  try {
    await telemetryCollection.add({
      ids: [message.id || ""],
      documents: [message.content],
      embeddings: messageEmbedding.value,
      metadatas: [metadata],
    });

    if (message.chunks) {
      const retrievedChunksRecord = message.chunks
        .map((chunk) => chunk.id)
        .join(", ");

      const embeddingResult = await embed([retrievedChunksRecord]);
      if (!embeddingResult.ok) {
        return embeddingResult;
      }

      await retrievedChunksCollection.add({
        ids: [uuidv4()],
        documents: [retrievedChunksRecord],
        embeddings: embeddingResult.value,
        metadatas: [{ message_id: message.id }],
      });
    }

    return { ok: true, value: message };
  } catch {
    return {
      ok: false,
      error: new AppError(
        "Failed to add a new record to the telemetry collection",
      ),
    };
  }
};

export const addChatRecord = async (
  chat: Chat,
): Promise<Result<Chat, AppError>> => {
  const clientResult = await getChromaClient();
  if (!clientResult.ok) {
    return clientResult;
  }

  const chatsCollectionResult = await getChromaCollection(
    clientResult.value,
    CHATS_COLLECTION,
  );
  if (!chatsCollectionResult.ok) {
    return chatsCollectionResult;
  }

  try {
    await chatsCollectionResult.value.add({
      ids: [chat.id],
      documents: [chat.title || ""],
      metadatas: [{ created: chat.created }],
    });

    return { ok: true, value: chat };
  } catch {
    return {
      ok: false,
      error: new AppError(
        `Failed to add a new chat. Verify that you are able to add data to your 'chats' collection.`,
      ),
    };
  }
};

/**
 * We use the data we persist in our Chroma collections to display previous chats in the app.
 * Every message in a chat is available in the 'telemetry' collection. For every message, we can
 * get the chunks retrieved for it, and their summaries.
 * @param chatId
 */
export const getChatMessages = async (
  chatId: string,
): Promise<Result<Message[], AppError>> => {
  const clientResult = await getChromaClient();
  if (!clientResult.ok) {
    return clientResult;
  }

  const collectionsResult = await getCollections(clientResult.value, [
    TELEMETRY_COLLECTION,
    RETRIEVED_CHUNKS_COLLECTION,
    SUMMARIES_COLLECTION,
    DATA_COLLECTION,
  ]);

  if (!collectionsResult.ok) {
    return collectionsResult;
  }

  const telemetryCollection = collectionsResult.value[TELEMETRY_COLLECTION];
  const retrievedChunksCollection =
    collectionsResult.value[RETRIEVED_CHUNKS_COLLECTION];
  const summariesCollection = collectionsResult.value[SUMMARIES_COLLECTION];
  const dataCollection = collectionsResult.value[DATA_COLLECTION];

  try {
    const chatMessages = await telemetryCollection.get({
      where: { chat_id: chatId },
    });

    const messagesResult = recordsToObject<Message>(
      chatMessages,
      messageMappingConfig,
      "Some records in the 'telemetry' collection do not match the shape required by this ap",
    );

    if (!messagesResult.ok) {
      return messagesResult;
    }

    const messages = messagesResult.value;

    const retrievedChunks = await retrievedChunksCollection.get({
      where: {
        message_id: {
          $in: messages.filter((m) => m.role === "assistant").map((m) => m.id),
        },
      },
    });

    const chunkIdsForMessage: Record<string, string[]> = {};
    retrievedChunks.metadatas.forEach((metadata, index) => {
      if (metadata !== null) {
        const messageId = metadata.message_id as string;
        chunkIdsForMessage[messageId] =
          retrievedChunks.documents[index]?.split(",").map((id) => id.trim()) ||
          [];
      }
    });

    const chunkIds = Object.values(chunkIdsForMessage).reduce(
      (ids, current) => {
        return [...ids, ...current];
      },
      [] as string[],
    );

    const chunkRecords = await dataCollection.get({ ids: chunkIds });
    const chunksResult = recordsToObject<Chunk>(
      chunkRecords,
      chunkMappingConfig,
      "Some records in the 'data' collection do not match the shape required by this app",
    );

    if (!chunksResult.ok) {
      return chunksResult;
    }

    const summariesRecords = await summariesCollection.get({
      where: {
        chunk_id: {
          $in: chunkIds,
        },
      },
    });

    const summaries = summariesRecords.documents.map((document, index) => {
      return {
        summary: document as string,
        chunkId: (summariesRecords.metadatas[index]?.chunk_id as string) || "",
      };
    });

    const chunks: Chunk[] = chunksResult.value.map((chunk) => {
      return {
        ...chunk,
        summary: summaries.find((s) => s.chunkId === chunk.id)?.summary,
      };
    });

    const messagesWithChunks = messages.map((m) => {
      if (m.role === "user") {
        return m;
      }
      const messageChunks: Chunk[] = chunkIdsForMessage[m.id].map(
        (chunkId) => chunks.find((chunk) => chunk.id === chunkId)!,
      );
      return { ...m, chunks: messageChunks };
    });

    return { ok: true, value: messagesWithChunks };
  } catch {
    return {
      ok: false,
      error: new AppError(`Failed to get messages for chat ${chatId}`),
    };
  }
};

export const appSetup = async () => {
  const clientResult = await getChromaClient();
  if (!clientResult.ok) {
    return clientResult;
  }
  try {
    return { ok: true, value: await clientResult.value.heartbeat() };
  } catch {
    return {
      ok: false,
      error: new AppError(
        "Failed to connect to a Chroma server. Are you sure it is running?",
      ),
    };
  }
};

export const createChat = async () => {
  return { id: uuidv4(), created: new Date().toISOString() } as Chat;
};
