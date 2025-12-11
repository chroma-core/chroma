import {
  convertToModelMessages,
  createUIMessageStream,
  createUIMessageStreamResponse,
  streamText,
  UIMessage,
} from "ai";
import { openai } from "@ai-sdk/openai";
import { queryMovies } from "@/lib/chroma";
import { SearchResultRow } from "chromadb";

export const maxDuration = 30;

type ContextItem = {
  documents: SearchResultRow[];
};

type MoviesContextMessage = UIMessage<never, { context: ContextItem }>;

export async function POST(req: Request) {
  const { messages }: { messages: UIMessage[] } = await req.json();

  const systemPrompt = [
    "You are helping a user chat with a movies dataset.",
    "Use the dataset description to understand the data and keep answers concise.",
    "<dataset_description>",
    `Dataset of 44,000+ movies released prior to 2017 from The Movies Dataset. Each record includes the title, overview, budget, original language, and release year. The collection uses dense embeddings and BM25, enabling hybrid search.`,
    "</dataset_description>",
  ].join("\n");

  const last = messages.findLast((m) => m.role === "user");
  const movieResults = await queryMovies(
    last!.parts
      .map((p) => {
        if (p.type !== "text") return "";
        return p.text;
      })
      .join("\n"),
  );

  const messagesWithContext = buildMessagesWithContext(messages, movieResults);

  const stream = createUIMessageStream<MoviesContextMessage>({
    execute: async ({ writer }) => {
      writer.write({
        type: "data-context",
        id: "context-1",
        data: {
          documents: movieResults.results,
        },
      });

      const result = streamText({
        model: openai("gpt-5-nano"),
        messages: convertToModelMessages(messagesWithContext),
        providerOptions: { openai: { reasoningEffort: "minimal" } },
        system: systemPrompt,
      });

      writer.merge(result.toUIMessageStream());
    },
  });

  return createUIMessageStreamResponse({ stream });
}

function buildMessagesWithContext(
  messages: UIMessage[],
  movieResults: { results: SearchResultRow[] },
) {
  const lastUserIndex = messages.findLastIndex((m) => m.role === "user");
  if (lastUserIndex === -1 || movieResults.results.length === 0) {
    return messages;
  }

  const summary = movieResults.results
    .map((row, index) => {
      const metadata =
        row.metadata && Object.keys(row.metadata).length > 0
          ? JSON.stringify(row.metadata)
          : "None";

      return `Result ${index + 1} (score: ${row.score ?? "n/a"}):\nDocument: ${row.document ?? "None"}\nMetadata: ${metadata}`;
    })
    .join("\n");

  const contextMessage: UIMessage = {
    id: "context",
    role: "system",
    parts: [
      {
        type: "text",
        text: `Dataset search context:\n${summary}`,
      },
    ],
  };

  const copy = [...messages];
  copy.splice(lastUserIndex, 0, contextMessage);

  return copy;
}
