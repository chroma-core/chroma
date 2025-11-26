import readline from "node:readline/promises";
import { retrievalWithMovies } from "./retrieval.js";
import { searchMovies } from "./search.js";

type Subcommand = "retrieval" | "search";

async function promptQuery(subcommand: Subcommand): Promise<string> {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  const prompt =
    subcommand === "retrieval" ? "Ask about movies: " : "Search for movies: ";
  const q = await rl.question(prompt);
  rl.close();
  return q;
}

function printUsage() {
  console.log("Usage: bun src/main.ts <command> [query]");
  console.log("");
  console.log("Commands:");
  console.log("  retrieval [query]  Ask questions about movies (LLM-powered)");
  console.log("  search [query]     Search for movies in the collection");
}

async function run() {
  const subcommand = process.argv[2] as Subcommand | undefined;

  if (!subcommand || (subcommand !== "retrieval" && subcommand !== "search")) {
    printUsage();
    process.exit(1);
  }

  const query =
    process.argv.slice(3).join(" ").trim() || (await promptQuery(subcommand));

  if (!query) {
    console.error("No query provided.");
    process.exit(1);
  }

  try {
    if (subcommand === "retrieval") {
      const result = await retrievalWithMovies(query);
      console.log(`Answer: ${result.answer}`);
      console.log(`Sources used: ${result.sourceCount}`);
    } else {
      const result = await searchMovies(query);
      console.log(`Found ${result.count} results:\n`);
      result.results.forEach((item, idx) => {
        console.log(`[${idx + 1}] (${item.id})`);
        console.log(`${item.document}\n`);
      });
    }
  } catch (err) {
    console.error((err as Error).message);
    process.exit(1);
  }
}

run();
