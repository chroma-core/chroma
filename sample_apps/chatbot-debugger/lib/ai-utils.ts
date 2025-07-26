"use server";

import { createOpenAI } from "@ai-sdk/openai";
import { generateText, streamText } from "ai";
import { AppError, Chunk, Message, Result } from "@/lib/types";
import { getOpenAIKey } from "@/lib/utils";
import { OpenAIEmbeddingFunction } from "chromadb";

export const generateStream = async (
  prompt: string,
): Promise<Result<AsyncIterable<string>, AppError>> => {
  const openAIKeyResult = getOpenAIKey();
  if (!openAIKeyResult.ok) {
    return openAIKeyResult;
  }
  const openai = createOpenAI({ apiKey: openAIKeyResult.value });

  const { textStream } = streamText({
    model: openai("gpt-4o"),
    prompt,
  });

  return { ok: true, value: textStream };
};

export const generate = async (
  prompt: string,
): Promise<Result<string, AppError>> => {
  const openAIKeyResult = getOpenAIKey();
  if (!openAIKeyResult.ok) {
    return openAIKeyResult;
  }
  const openai = createOpenAI({ apiKey: openAIKeyResult.value });
  try {
    const { text } = await generateText({
      model: openai("gpt-4o"),
      prompt,
    });

    return { ok: true, value: text };
  } catch {
    return { ok: false, error: new AppError("Failed to generate text") };
  }
};

export const getAssistantResponse = async (
  userMessage: Message,
  chunks: Chunk[],
  chatHistory: Message[],
): Promise<Result<AsyncIterable<string>, AppError>> => {
  let prompt = `User: ${userMessage.content}`;
  prompt += `\n\nContext:\n ${chunks.map((c) => c.content).join("\n")}`;
  prompt += `\n\nChat History:\n${chatHistory.map((m) => `${m.role}: ${m.content}`).join("\n")}`;

  return await generateStream(prompt);
};

export const getChatTitle = async (message: string) => {
  const prompt = `Provide a short title for a conversation where this is the first message from the user: ${message}. Return only the title and nothing else. Do not wrap your answer in quotes.`;
  return await generate(prompt);
};

export const getChunkSummary = async (
  chunk: string,
  chunkingPrompt?: string,
) => {
  const prompt = `Please provide a 1 sentence short summary of the following.\n${chunkingPrompt && `${chunkingPrompt}\n`}Summarize: ${chunk}`;
  return await generate(prompt);
};

export const getOpenAIEF = async (
  model: string = "text-embedding-3-large",
): Promise<Result<OpenAIEmbeddingFunction, AppError>> => {
  const apiKeyResult = getOpenAIKey();
  if (!apiKeyResult.ok) {
    return apiKeyResult;
  }
  return {
    ok: true,
    value: new OpenAIEmbeddingFunction({
      openai_api_key: apiKeyResult.value,
      openai_model: model,
    }),
  };
};
