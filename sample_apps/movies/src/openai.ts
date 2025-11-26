import OpenAI from "openai";

export function getOpenAIClient() {


  const apiKey = process.env.OPENAI_API_KEY;

  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is required");
  }

  const client = new OpenAI({ apiKey });
  return client;
}
