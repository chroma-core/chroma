import { promises as fs } from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ChromaClient } from "chromadb";
import "dotenv/config";
// @ts-ignore
import { Collection } from "chromadb/src/Collection";
import {
  RecursiveCharacterTextSplitter,
  TokenTextSplitter,
} from "@langchain/textsplitters";
import { v4 as uuidv4 } from "uuid";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const splitMarkdownByHeaders = (
  markdownContent: string,
): {
  title: string;
  content: string;
}[] => {
  const normalizedContent = markdownContent.replace(/\r\n/g, "\n");
  const lines = normalizedContent.split("\n");

  const sections: {
    content: string;
    title: string;
  }[] = [];
  let currentSection: string[] = [];
  let currentTitle = "";
  let hasStarted = false;
  let insideCodeFence = false;
  let currentFenceMarker = "";

  const addCurrentSection = () => {
    if (currentSection.length > 0 && hasStarted) {
      sections.push({
        content: currentSection.join("\n").trim(),
        title: currentTitle,
      });
      currentSection = [];
    }
  };

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    const codeFenceMatch = line.match(/^([`~]{3,})/);
    if (codeFenceMatch) {
      if (!insideCodeFence) {
        insideCodeFence = true;
        currentFenceMarker = codeFenceMatch[1][0]; // Remember if it's ` or ~
      } else if (line.startsWith(currentFenceMarker)) {
        insideCodeFence = false;
      }
      currentSection.push(line);
      continue;
    }

    if (insideCodeFence) {
      currentSection.push(line);
      continue;
    }

    const headerMatch = line.match(/^(#{1,2})\s+(.+)$/);

    if (headerMatch) {
      const [fullMatch, hashes, headerText] = headerMatch;

      if (hashes.length <= 2) {
        addCurrentSection();
        currentTitle = headerText.trim();
        currentSection.push(line);
        hasStarted = true;
      } else {
        currentSection.push(line);
      }
    } else {
      currentSection.push(line);
    }
  }

  addCurrentSection();
  return sections;
};

export const recursiveChunker = async (
  inputData: string,
  chunkSize: number,
  chunkOverlap: number,
): Promise<{ chunk: string; title: string }[]> => {
  const markdocTagPattern = /{%\s.*?\s%}/g;
  const data = inputData.replace(markdocTagPattern, "");

  const sections = splitMarkdownByHeaders(data);
  const splitter = new RecursiveCharacterTextSplitter({
    chunkSize,
    chunkOverlap,
    separators: ["\n\n", "\n", ".", " ", ""],
  });

  const results: { chunk: string; title: string }[] = [];
  for (const { title, content } of sections) {
    const sectionChunks = await splitter.splitText(content);
    for (const c of sectionChunks) {
      results.push({ chunk: c, title });
    }
  }

  return results;
};

const tokenChunker = async (
  data: { chunk: string; title: string },
  chunkSize: number,
  chunkOverlap: number,
) => {
  const splitter = new TokenTextSplitter({ chunkSize, chunkOverlap });
  return (await splitter.splitText(data.chunk)).map((chunk) => {
    return { chunk, title: data.title };
  });
};

const ingestDocs = async (
  collection: Collection,
  filePath: string,
  pageLink: string,
) => {
  try {
    const content = await fs.readFile(filePath, "utf8");

    const match = content.match(/^# (.+)/m);
    const pageTitle = match ? match[1].trim() : "Chroma Docs";

    const splitTexts = await recursiveChunker(content, 1000, 100);
    const tokenSplitTexts: { chunk: string; title: string }[] = [];
    for (const chunk of splitTexts) {
      tokenSplitTexts.push(...(await tokenChunker(chunk, 256, 0)));
    }

    const path = pageLink.split("/").slice(1);

    await collection.add({
      ids: tokenSplitTexts.map(() => uuidv4()),
      documents: tokenSplitTexts.map((chunk) => chunk.chunk),
      metadatas: tokenSplitTexts.map((chunk) => {
        return {
          section: path[0],
          subsection: path.length === 3 ? path[1] : undefined,
          page: path.length === 3 ? path[2] : path[1],
          title: chunk.title,
          pageTitle,
        };
      }),
    });
  } catch (err) {
    console.error("Error ingesting file:", `${pageLink}\n${err}`);
  }
};

const collectMarkdownFiles = async (
  dir: string,
  allFiles: string[],
): Promise<void> => {
  const entries = await fs.readdir(dir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      await collectMarkdownFiles(fullPath, allFiles);
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      allFiles.push(fullPath);
    }
  }
};

const main = async (): Promise<void> => {
  const markdocContentDir = path.join(__dirname, "..", "markdoc", "content");
  const allMarkdowns: string[] = [];
  await collectMarkdownFiles(markdocContentDir, allMarkdowns);

  const chromaClient = new ChromaClient({
    path: "https://api.trychroma.com:8000",
    auth: {
      provider: "token",
      credentials: process.env.CHROMA_CLOUD_API_KEY,
      tokenHeaderType: "X_CHROMA_TOKEN",
    },
    tenant: process.env.CHROMA_CLOUD_TENANT,
    database: "docs",
  });

  const collection: Collection = await chromaClient.getOrCreateCollection({
    name: "docs-content",
  });

  for (const doc of allMarkdowns) {
    await ingestDocs(
      collection,
      doc,
      doc.replace(markdocContentDir, "").replace(".md", ""),
    );
  }
};

if (import.meta.url === `file://${__filename}`) {
  main().catch((err) => console.error("Error:", err));
}

export default main;
