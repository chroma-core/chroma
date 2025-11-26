import { getMoviesCollection } from "./client.js";
import { getOpenAIClient } from "./openai.js";

export async function retrievalWithMovies(query: string) {
  const collection = await getMoviesCollection();

  const results = await collection.query({
    queryTexts: [query],
  });

  const docs = results.documents?.[0] ?? [];
  const ids = results.ids?.[0] ?? [];

  const context = docs
    .map((doc, idx) => `(${ids[idx] ?? idx + 1}) ${doc}`)
    .join("\n");

  const client = getOpenAIClient();

  const completion = await client.chat.completions.create({
    model: "gpt-4o-mini",
    messages: [
      {
        role: "system",
        content:
          "You are a movie guide. Use the provided context to answer concisely. If unsure, say you do not know.",
      },
      {
        role: "user",
        content: `Context:\n${context}\n\nQuestion: ${query}`,
      },
    ],
  });

  return {
    answer: completion.choices[0]?.message?.content ?? "",
    sourceCount: docs.length,
  };
}
