"use server";

import {
  recordsToObject,
} from "@/lib/utils";
import { getChromaClient, getChromaCollection } from "@/lib/server-utils";
import { AppError, Chunk, chunkMappingConfig, Result } from "@/lib/types";
import { getOpenAIEF } from "@/lib/ai-utils";
import {DATA_COLLECTION, SUMMARIES_COLLECTION} from "@/lib/constants";

/**
 *
 * @param texts An array of strings to embed using the OpenAIEmbeddingFunction
 * @return An array of embeddings for the input array
 */
export const embed = async (
  texts: string[],
): Promise<Result<number[][], AppError>> => {
  const embeddingFunctionResult = await getOpenAIEF();
  if (!embeddingFunctionResult.ok) {
    return embeddingFunctionResult;
  }

  const embeddingFunction = embeddingFunctionResult.value;

  try {
    const response = await embeddingFunction.generate(texts);
    return { ok: true, value: response };
  } catch {
    return { ok: false, error: new AppError("Failed to create embeddings") };
  }
};

/**
 * Retrieves documents relevant to the user message from your 'data' collection. For each
 * retrieved document, we also get its summary from the 'summaries' collection. This will allow
 * us to show users what data was used as context for their query, as the raw documents form the
 * 'data' collection can be chopped code or documentation texts.
 * @param messageContent The user message
 */
export const retrieveChunks = async (
  messageContent: string,
): Promise<Result<Chunk[], AppError>> => {
  const clientResult = await getChromaClient();
  if (!clientResult.ok) {
    return clientResult;
  }

  const dataCollectionResult = await getChromaCollection(
    clientResult.value,
    DATA_COLLECTION,
  );
  if (!dataCollectionResult.ok) {
    return dataCollectionResult;
  }

  const summariesCollectionResult = await getChromaCollection(
    clientResult.value,
    SUMMARIES_COLLECTION,
  );
  if (!summariesCollectionResult.ok) {
    return summariesCollectionResult;
  }

  // Alternatively, you can send the message directly to the 'query' function using the
  // 'queryTexts' argument.
  const queryEmbeddingResult = await embed([messageContent]);
  if (!queryEmbeddingResult.ok) {
    return queryEmbeddingResult;
  }

  let result;
  try {
    result = await dataCollectionResult.value.query({
      queryEmbeddings: queryEmbeddingResult.value,
      nResults: 5,
    });
  } catch (e) {
    console.error(e);
    return {
      ok: false,
      error: new AppError("Failed to query the 'data' collection"),
    };
  }

  const chunkIds = result.ids[0];

  const summaries: Record<string, string> = {};
  try {
    const summariesResult = await summariesCollectionResult.value.get({
      where: { chunk_id: { $in: chunkIds } },
    });

    summariesResult.metadatas.forEach((m, i) => {
      const { chunk_id } = m as { chunk_id: string };
      summaries[chunk_id] = summariesResult.documents[i] || "";
    });
  } catch {
    return {
      ok: false,
      error: new AppError(
        `Failed to fetch chunk summaries for chunks: ${chunkIds.join(", ")}`,
      ),
    };
  }

  const chunksResults = recordsToObject<Chunk>(
    {
      ids: result.ids[0],
      documents: result.documents[0],
      metadatas: result.metadatas[0],
    },
    chunkMappingConfig,
    "Some records in the data collection were corrupted. Please make sure they contain all the required fields for the app",
  );

  if (!chunksResults.ok) {
    return chunksResults;
  }

  return {
    ok: true,
    value: chunksResults.value.map((chunk) => {
      return { ...chunk, summary: summaries[chunk.id] };
    }),
  };
};
