"use server";

import { generateText } from "ai";
import { Message } from "@/lib/models";
import { createAnthropic } from "@ai-sdk/anthropic";

export const generate = async (prompt: string) => {
  const anthropic = createAnthropic({
    apiKey: process.env.ANTHROPIC_API_KEY,
  });
  const { text } = await generateText({
    model: anthropic("claude-3-7-sonnet-20250219"),
    prompt,
  });
  return text;
};

export const getAssistantResponse = async (
  userMessage: Message,
  conversation: Message[],
) => {
  const messages = [
    ...conversation
      .sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp))
      .filter((m) => m.id !== userMessage.id),
  ];

  let prompt = `User: ${userMessage.content}`;

  if (messages.length > 0) {
    prompt +=
      "\nHere is the conversation history. You can use it if it is relevant to the user prompt.\n";
    prompt += messages.map((m) => m.content).join("\n");
  }

  return await generate(prompt);
};
