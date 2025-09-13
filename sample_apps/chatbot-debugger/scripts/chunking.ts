import { RecursiveCharacterTextSplitter } from "@langchain/textsplitters";
import { v4 as uuidv4 } from "uuid";
import { getChunkSummary } from "@/lib/ai-utils";
import { Result } from "@/lib/types";

// Supported file types
export const SUPPORTED_FILE_EXTENSIONS: string[] = [
  ".md",
  ".mdx",
  ".py",
  ".ts",
  ".js",
];

// Prompt to help models produce chunk summaries
const CHUNKING_PROMPT =
  "This data is from the documentation of Chroma with some source code examples from the chromadb Python package.";

const MARKDOWN_CHUNK_SIZE: number = 1000;
const MARKDOWN_CHUNK_OVERLAP: number = 100;

const CODE_CHUNK_SIZE: number = 1200;
const CODE_CHUNK_OVERLAP: number = 120;

enum ChunkType {
  Code = "code",
  Docs = "docs",
}

enum Language {
  Python = "python",
  Javascript = "javascript",
}

interface FileChunk {
  id: string;
  document: string;
  type: ChunkType;
  fileName: string;
  summary: string;
  language?: Language;
}

const mdSplitter = RecursiveCharacterTextSplitter.fromLanguage("markdown", {
  chunkSize: MARKDOWN_CHUNK_SIZE,
  chunkOverlap: MARKDOWN_CHUNK_OVERLAP,
});

const pySplitter = RecursiveCharacterTextSplitter.fromLanguage("python", {
  chunkSize: CODE_CHUNK_SIZE,
  chunkOverlap: CODE_CHUNK_OVERLAP,
});

const jsSplitter = RecursiveCharacterTextSplitter.fromLanguage("js", {
  chunkSize: CODE_CHUNK_SIZE,
  chunkOverlap: CODE_CHUNK_OVERLAP,
});

const chunkCode = async (
  fileName: string,
  fileContent: string,
  language: Language,
  prompt?: string,
) => {
  const splitter = language === Language.Python ? pySplitter : jsSplitter;
  return Promise.all(
    (await splitter.createDocuments([fileContent])).map(async (doc) => {
      const chunkingPrompt =
        prompt && `${prompt}. This is a code snippet from the file ${fileName}`;

      const summary = await getChunkSummary(doc.pageContent, chunkingPrompt);
      if (!summary.ok) {
        throw new Error("Failed to generate chunk summary");
      }

      const document = `${language === Language.Python ? "#" : "//"} From ${fileName}}.\n${doc.pageContent}`;

      return {
        id: uuidv4(),
        document,
        type: ChunkType.Code,
        fileName,
        summary: summary.value,
      } as FileChunk;
    }),
  );
};

const chunkMarkdwon = async (
  fileName: string,
  fileContent: string,
  prompt?: string,
) => {
  return await Promise.all(
    (await mdSplitter.createDocuments([fileContent])).map(async (doc) => {
      const document = `From ${fileName}:\n${doc.pageContent}`;
      const chunkingPrompt =
        prompt &&
        `${prompt}. This is a Markdown snippet from the file ${fileName}`;

      const summary = await getChunkSummary(doc.pageContent, chunkingPrompt);
      if (!summary.ok) {
        throw new Error("Failed to generate chunk summary");
      }
      return {
        id: uuidv4(),
        document,
        type: ChunkType.Docs,
        fileName,
        summary: summary.value,
      } as FileChunk;
    }),
  );
};

export const chunkFile = async (
  fileName: string,
  fileContent: string,
): Promise<Result<FileChunk[], Error>> => {
  const fileExtension = `.${fileName.split(".").pop() || ""}`;
  if (!SUPPORTED_FILE_EXTENSIONS.includes(fileExtension)) {
    return {
      ok: false,
      error: new Error(
        `Unsupported file type. We currently only support: ${SUPPORTED_FILE_EXTENSIONS.join(", ")}`,
      ),
    };
  }

  try {
    let chunks;
    switch (fileExtension) {
      case ".py":
        chunks = await chunkCode(
          fileName,
          fileContent,
          Language.Python,
          CHUNKING_PROMPT,
        );
        break;
      case ".js":
        chunks = chunks = await chunkCode(
          fileName,
          fileContent,
          Language.Javascript,
          CHUNKING_PROMPT,
        );
        break;
      default:
        chunks = await chunkMarkdwon(fileName, fileContent, CHUNKING_PROMPT);
        break;
    }

    return { ok: true, value: chunks };
  } catch {
    return { ok: false, error: new Error(`Failed to chunk ${fileName}`) };
  }
};
