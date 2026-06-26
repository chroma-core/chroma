import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import {
  AppError,
  AppParams,
  AuthOptions,
  ChromaGetResult,
  MappingConfig,
  Result,
} from "@/lib/types";

export const cn = (...inputs: ClassValue[]) => {
  return twMerge(clsx(inputs));
};

export const formatToK = (num: number) => {
  return `${Math.round(num / 1000)}k`;
};

export const getOpenAIKey = (): Result<string, AppError> => {
  const openAIKey = process.env.OPENAI_API_KEY;
  if (!openAIKey) {
    return {
      ok: false,
      error: new AppError(
        "This app requires an OpenAI API key. Please set one in your .env file.",
      ),
    };
  }
  return { ok: true, value: openAIKey };
};

export const getAppParams = (settings?: {
  requireCloud: boolean;
}): Result<AppParams, AppError> => {
  if (settings?.requireCloud && !process.env.CHROMA_CLOUD_API_KEY) {
    return {
      ok: false,
      error: new AppError("CHROMA_CLOUD_API_KEY is not set"),
    };
  }

  const auth = process.env.CHROMA_CLOUD_API_KEY
    ? ({
        provider: "token",
        credentials: process.env.CHROMA_CLOUD_API_KEY,
        tokenHeaderType: "X_CHROMA_TOKEN",
      } as AuthOptions)
    : undefined;

  const openAIKeyResult = getOpenAIKey();
  if (!openAIKeyResult.ok) {
    return openAIKeyResult;
  }

  const requiredParams: Record<string, string | undefined> = {
    CHROMA_HOST: process.env.CHROMA_HOST,
    CHROMA_TENANT: process.env.CHROMA_TENANT,
    CHROMA_DB_NAME: process.env.CHROMA_DB_NAME,
    OPENAI_API_KEY: openAIKeyResult.value,
  };

  const missingParams = Object.entries(requiredParams)
    .filter(
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      ([_, value]) => !value,
    )
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    .map(([key, _]) => key);

  if (missingParams.length > 0) {
    return {
      ok: false,
      error: new AppError(
        `Missing required environment variables for this app. Please set them in your .env file: ${missingParams.join(", ")}.`,
      ),
    };
  }

  return {
    ok: true,
    value: {
      chromaClientParams: {
        auth,
        path: requiredParams.CHROMA_HOST,
        tenant: requiredParams.CHROMA_TENANT,
        database: requiredParams.CHROMA_DB_NAME,
      },
      openAIKey: requiredParams.OPENAI_API_KEY!,
    },
  };
};

export const recordsToObject = <T>(
  result: ChromaGetResult,
  mapping: MappingConfig<T>,
  errorMessage: string,
  validator?: (obj: T) => boolean,
): Result<T[], AppError> => {
  try {
    const mappedObjects = result.ids.map((_, i) => {
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
          throw new AppError(errorMessage);
        }

        obj[key] = value as T[typeof key];
      });

      if (validator && !validator(obj)) {
        throw new AppError(errorMessage);
      }

      return obj;
    });
    return { ok: true, value: mappedObjects };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof AppError ? e : new AppError(String(e)),
    };
  }
};
