import {
  convertToModelMessages,
  streamText,
  UIMessage,
  tool,
  stepCountIs,
} from "ai";
import { openai } from "@ai-sdk/openai";
import { queryMovies } from "@/lib/chroma";
import { z } from "zod";

export const maxDuration = 30;

export async function POST(req: Request) {
  const { messages }: { messages: UIMessage[] } = await req.json();

  const systemPrompt = [
    "You are helping a user chat with a movies dataset.",
    "Use the searchMovies tool to find relevant movies when needed.",
    "<dataset_description>",
    "Dataset of 44,000+ movies released prior to 2017 from The Movies Dataset. Each record includes the title, overview, budget, original language, and release year. The collection uses dense embeddings and BM25, enabling hybrid search.",
    "</dataset_description>",
  ].join("\n");

  const result = streamText({
    model: openai("gpt-5-nano"),
    messages: convertToModelMessages(messages),
    providerOptions: { openai: { reasoningEffort: "minimal" } },
    system: systemPrompt,
    tools: {
      searchMovies: tool({
        description: "Search the movies dataset for relevant films",
        inputSchema: z.object({
          query: z
            .string()
            .describe("The search query to find relevant movies"),
        }),
        execute: async ({ query }) => queryMovies(query),
      }),
    },
    stopWhen: stepCountIs(5),
  });

  return result.toUIMessageStreamResponse();
}
