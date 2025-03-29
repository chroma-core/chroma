import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import { AuthOptions, ChromaGetResult, MappingConfig } from "@/lib/types";
import { ChromaClientParams } from "chromadb";

export const CHATS_COLLECTION = "chats";
export const TELEMETRY_COLLECTION = "telemetry";
export const DATA_COLLECTION = "data";

export const cn = (...inputs: ClassValue[]) => {
  return twMerge(clsx(inputs));
};

export const formatToK = (num: number) => {
  return `${Math.round(num / 1000)}k`;
};

export const getAppParams = (): {
  chromaClientParams: ChromaClientParams;
  openAIKey: string;
} => {
  const auth = process.env.CHROMA_CLOUD_API_KEY
    ? ({
        provider: "token",
        credentials: process.env.CHROMA_CLOUD_API_KEY,
        tokenHeaderType: "X_CHROMA_TOKEN",
      } as AuthOptions)
    : undefined;

  const requiredParams: Record<string, string | undefined> = {
    CHROMA_HOST: process.env.CHROMA_HOST,
    CHROMA_TENANT: process.env.CHROMA_TENANT,
    CHROMA_DB_NAME: process.env.CHROMA_DB_NAME,
    OPENAI_API_KEY: getOpenAIKey(),
  };

  const missingParams = Object.entries(requiredParams)
    .filter(
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      ([_, value]) => !value,
    )
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .map(([key, _]) => key);

  if (missingParams.length > 0) {
    throw new Error(
      `Missing required environment variables for this app. Please set them in your .env file: ${missingParams.join(", ")}.`,
    );
  }

  return {
    chromaClientParams: {
      auth,
      path: requiredParams.CHROMA_HOST,
      tenant: requiredParams.CHROMA_TENANT,
      database: requiredParams.CHROMA_DB_NAME,
    },
    openAIKey: requiredParams.OPENAI_API_KEY!,
  };
};

export const getOpenAIKey = () => {
  const openAIKey = process.env.OPENAI_API_KEY;
  if (!openAIKey) {
    throw new Error(
      "This app requires an OpenAI API key. Please set OPENAI_API_KEY in your .env file",
    );
  }
  return openAIKey;
};

export const recordsToObject = <T>(
  result: ChromaGetResult,
  mapping: MappingConfig<T>,
  errorMessage: string,
  validator?: (obj: T) => boolean,
): T[] =>
  result.ids.map((_, i) => {
    const obj = {} as T;

    (Object.keys(mapping) as Array<keyof T>).forEach((key) => {
      const rule = mapping[key];

      let value: unknown;

      if (rule.from === "ids") {
        value = result.ids[i];
      } else if (rule.from === "documents") {
        value = result.documents?.[i];
      } else if (rule.from === "metadatas") {
        value = result.metadatas?.[i]?.[rule.key];
      }

      if (value === null) {
        throw new Error(errorMessage);
      }

      obj[key] = value as T[typeof key];
    });

    if (validator && !validator(obj)) {
      throw new Error(errorMessage);
    }

    return obj;
  });
