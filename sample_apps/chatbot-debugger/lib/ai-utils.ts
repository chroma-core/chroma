"use server";

import { createOpenAI } from "@ai-sdk/openai";
import { streamText } from "ai";
import { Chunk, Message } from "@/lib/types";

export const generate = async (prompt: string) => {
  const openai = createOpenAI({
    apiKey: process.env.OPENAI_API_KEY,
  });
  const { textStream } = streamText({
    model: openai("gpt-4o"),
    prompt,
  });

  return textStream;
};

export const getAssistantResponse = async (
  userMessage: Message,
  chunks: Chunk[],
) => {
  let prompt = `User: ${userMessage.content}`;
  prompt += `\n\nContext:\n ${chunks.map((c) => c.content).join("\n")}`;
  prompt +=
    "If the context does not seem relevant to the question, return only 'Sorry, I cannot answer.' Do not mention the fact that you were provided context.";

  return await generate(prompt);
};
