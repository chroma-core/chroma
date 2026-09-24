import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import "dotenv/config";
import { CLOUD_HOST, timeout } from "@/scripts/utils";
import * as fs from "fs";
import { getAllFiles, LocalFile, readFileContent } from "@/scripts/files";
import { chunkFile } from "@/scripts/chunking";
import { Collection, DefaultEmbeddingFunction } from "chromadb";
import { getChromaClient } from "@/lib/server-utils";
import { v4 as uuidv4 } from "uuid";
import { AppParams, Result } from "@/lib/types";
import ora from "ora";
import { getOpenAIEF } from "@/lib/ai-utils";
import { getAppParams } from "@/lib/utils";
import readline from "readline/promises";
import { stdin as input, stdout as output } from "process";
import { embed } from "@/lib/retrieval";

const argv = yargs(hideBin(process.argv))
  .usage(
    "Usage: npm run ingest -- --collection [collection_name] --root [path] --extensions [exts] --directories [dirs]",
  )
  .options({
    collection: {
      description:
        "The name of the Chroma collection to ingest data into. Will be used to create the [NAME]-data and [NAME]-summaries collections.",
      alias: "c",
      type: "string",
      demandOption: true,
      default: "chroma-docs",
    },
    root: {
      description: "Root directory path",
      alias: "r",
      type: "string",
      demandOption: true,
    },
    extensions: {
      description: 'Space-separated list of file extensions (e.g., ".md .py")',
      alias: "e",
      type: "array",
      default: [".md", ".py"],
    },
    directories: {
      description: "Space-separated list of directories to process",
      alias: "d",
      type: "array",
      default: [
        "docs/docs.trychroma.com",
        "chromadb/utils/embedding_functions",
      ],
    },
  })
  .example(
    "npm run ingest -- --root ./chroma --extensions .md .py --directories docs/docs.trychroma.com",
    "Process markdown and Python files from docs directory in local folder",
  )
  .help()
  .alias("help", "h").argv as {
  collection: string;
  root: string;
  extensions?: string[];
  directories?: string[];
};

const processFile = async (
  file: LocalFile,
  dataCollection: Collection,
  summariesCollection: Collection,
) => {
  const fileReadResult = readFileContent(file);
  if (!fileReadResult.ok) {
    return fileReadResult;
  }

  const chunkingResult = await chunkFile(file.name, fileReadResult.value);
  if (!chunkingResult.ok) {
    return chunkingResult;
  }

  const chunks = chunkingResult.value;

  try {
    await dataCollection.add({
      ids: chunks.map((chunk) => chunk.id),
      documents: chunks.map((chunk) => chunk.document),
      metadatas: chunks.map((chunk) => {
        return { type: chunk.type.toString(), file: chunk.fileName };
      }),
    });
  } catch {
    return {
      ok: false,
      error: new Error(
        `Failed to add records to the ${dataCollection.name} collection`,
      ),
    };
  }

  const summaries = chunks.map((chunk) => chunk.summary);
  const embeddingsResult = await embed(summaries);
  if (!embeddingsResult.ok) {
    return embeddingsResult;
  }

  try {
    await summariesCollection.add({
      ids: chunks.map(() => uuidv4()),
      documents: summaries,
      embeddings: embeddingsResult.value,
      metadatas: chunks.map((chunk) => {
        return { chunk_id: chunk.id };
      }),
    });
  } catch {
    return {
      ok: false,
      error: new Error(
        `Failed to add records to the ${summariesCollection.name} collection`,
      ),
    };
  }
};

const verifyIngestSetup = async (
  rootPath: string,
  collectionName: string,
): Promise<Result<AppParams, Error>> => {
  if (!rootPath) {
    return {
      ok: false,
      error: new Error(
        "A valid root directory path is missing. Please provide it using the --root argument, or set the ROOT constant at the top of ingest.ts",
      ),
    };
  }

  if (!fs.existsSync(rootPath)) {
    return {
      ok: false,
      error: new Error(`The specified root path does not exist: ${rootPath}`),
    };
  }

  if (!collectionName) {
    return {
      ok: false,
      error: new Error(
        "Missing collection name. Please provide it as an argument or set the COLLECTION_NAME constant.",
      ),
    };
  }

  const appParamsResult = getAppParams({
    requireCloud: (process.env.CHROMA_HOST || "").includes(CLOUD_HOST),
  });

  if (!appParamsResult.ok) {
    return appParamsResult;
  }

  const chromaClientResult = await getChromaClient();
  if (!chromaClientResult.ok) {
    return chromaClientResult;
  }

  const chromaClient = chromaClientResult.value;
  try {
    await chromaClient.heartbeat();
  } catch {
    return {
      ok: false,
      error: new Error(
        `Cannot connect to your Chroma server at ${process.env.CHROMA_HOST}`,
      ),
    };
  }

  const appCollections: Record<string, boolean> = {
    [`${collectionName}-data`]: false,
    [`${collectionName}-summaries`]: false,
  };

  for (const name of Object.keys(appCollections)) {
    try {
      await chromaClient.getCollection({
        name,
        embeddingFunction: new DefaultEmbeddingFunction(),
      });
      appCollections[name] = true;
    } catch {}
  }

  const existing = Object.entries(appCollections)
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .filter(([_, v]) => v)
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .map(([name, _]) => name);

  if (existing.length > 0) {
    const rl = readline.createInterface({ input, output });
    let message: string;
    if (existing.length > 1) {
      message = `collections ${existing[0]} and ${existing[1]}`;
    } else {
      message = `collection ${existing[0]}`;
    }
    const confirm = await rl.question(
      `Your Chroma DB already has ${message}. Would you like to proceed with ingesting new data? Y/n\n`,
    );
    rl.close();
    if (confirm.toLowerCase().trim() !== "y") {
      return { ok: false, error: new Error("Ingest cancelled") };
    }
    console.log();
  }

  return appParamsResult;
};

const ingest = async (
  rootPath: string,
  allowedDirectories: string[],
  allowedExtensions: string[],
  collectionName: string,
): Promise<Result<undefined, Error>> => {
  const chromaClientResult = await getChromaClient();
  if (!chromaClientResult.ok) {
    return chromaClientResult;
  }

  const chromaClient = chromaClientResult.value;
  const openAIEFResult = await getOpenAIEF();
  if (!openAIEFResult.ok) {
    return openAIEFResult;
  }

  let dataCollection: Collection;
  let summariesCollection: Collection;

  try {
    dataCollection = await chromaClient.getOrCreateCollection({
      name: `${collectionName}-data`,
      embeddingFunction: openAIEFResult.value,
    });
  } catch {
    return {
      ok: false,
      error: new Error(
        `Failed to get or create the ${collectionName}-data collection`,
      ),
    };
  }

  try {
    summariesCollection = await chromaClient.getOrCreateCollection({
      name: `${collectionName}-summaries`,
    });
  } catch {
    return {
      ok: false,
      error: new Error(
        `Failed to get or create the ${collectionName}-summaries collection`,
      ),
    };
  }

  const collectSpinner = ora("Collecting files...").start();
  await timeout(2000);
  const filesResult = getAllFiles(
    rootPath,
    "",
    allowedExtensions,
    allowedDirectories,
  );

  if (!filesResult.ok) {
    return filesResult;
  }

  collectSpinner.stop();
  const files = filesResult.value;
  console.log(`✅  Collected ${files.length} files\n`);

  const processSpinner = ora("Processing files...").start();
  let i = 1;
  for (const file of files) {
    processSpinner.text = `Processing ${file.path} (${i}/${files.length})`;
    await processFile(file, dataCollection, summariesCollection);
    i += 1;
  }
  processSpinner.stop();

  console.log(`✅  Processing complete!`);
  return { ok: true, value: undefined };
};

const main = async () => {
  const rootPath = argv.root;
  const collectionName = argv.collection;

  const setupResult = await verifyIngestSetup(rootPath, collectionName);
  if (!setupResult.ok) {
    console.error(setupResult.error.message);
    return;
  }

  const allowedDirectories = argv.directories || [];
  const allowedExtensions = argv.extensions || [];

  console.log("Starting to process local files");
  console.log(`Root directory: ${rootPath}`);
  console.log(
    `File extensions: ${allowedExtensions.length > 0 ? allowedExtensions.join(", ") : "All"}`,
  );
  console.log(
    `Directories: ${allowedDirectories.length > 0 ? allowedDirectories.join(", ") : "All"}`,
  );
  console.log("--------------------------------------------");
  await timeout(2000);

  const result = await ingest(
    rootPath,
    allowedDirectories,
    allowedExtensions,
    collectionName,
  );

  console.log();

  if (!result.ok) {
    console.error(result.error.message);
    return;
  }
};

main().finally();
