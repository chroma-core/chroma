// Import schemas directly
import amazonBedrockSchema from "../../../../../../schemas/embedding_functions/amazon_bedrock.json";
import baseSchema from "../../../../../../schemas/embedding_functions/base_schema.json";
import chromaLangchainSchema from "../../../../../../schemas/embedding_functions/chroma_langchain.json";
import cohereSchema from "../../../../../../schemas/embedding_functions/cohere.json";
import defaultSchema from "../../../../../../schemas/embedding_functions/default.json";
import googleGenerativeAiSchema from "../../../../../../schemas/embedding_functions/google_generative_ai.json";
import googlePalmSchema from "../../../../../../schemas/embedding_functions/google_palm.json";
import googleVertexSchema from "../../../../../../schemas/embedding_functions/google_vertex.json";
import huggingfaceSchema from "../../../../../../schemas/embedding_functions/huggingface.json";
import huggingfaceServerSchema from "../../../../../../schemas/embedding_functions/huggingface_server.json";
import instructorSchema from "../../../../../../schemas/embedding_functions/instructor.json";
import jinaSchema from "../../../../../../schemas/embedding_functions/jina.json";
import ollamaSchema from "../../../../../../schemas/embedding_functions/ollama.json";
import onnxMiniLmL6V2Schema from "../../../../../../schemas/embedding_functions/onnx_mini_lm_l6_v2.json";
import openClipSchema from "../../../../../../schemas/embedding_functions/open_clip.json";
import openaiSchema from "../../../../../../schemas/embedding_functions/openai.json";
import roboflowSchema from "../../../../../../schemas/embedding_functions/roboflow.json";
import sentenceTransformerSchema from "../../../../../../schemas/embedding_functions/sentence_transformer.json";
import text2vecSchema from "../../../../../../schemas/embedding_functions/text2vec.json";
import transformersSchema from "../../../../../../schemas/embedding_functions/transformers.json";
import voyageaiSchema from "../../../../../../schemas/embedding_functions/voyageai.json";

import Ajv from "ajv";

// Define a common interface for all schemas
interface Schema {
  $schema: string;
  title?: string;
  description?: string;
  version?: string;
  type: string;
  properties: Record<string, any>;
  required?: string[];
  additionalProperties?: boolean;
  [key: string]: any; // Allow for other properties
}

const ajv = new Ajv({
  strict: false, // Allow unknown keywords
  allErrors: true,
});

// Map of schema names to schema objects
const schemaMap = {
  amazon_bedrock: amazonBedrockSchema as Schema,
  base_schema: baseSchema as Schema,
  chroma_langchain: chromaLangchainSchema as Schema,
  cohere: cohereSchema as Schema,
  default: defaultSchema as Schema,
  google_generative_ai: googleGenerativeAiSchema as Schema,
  google_palm: googlePalmSchema as Schema,
  google_vertex: googleVertexSchema as Schema,
  huggingface: huggingfaceSchema as Schema,
  huggingface_server: huggingfaceServerSchema as Schema,
  instructor: instructorSchema as Schema,
  jina: jinaSchema as Schema,
  ollama: ollamaSchema as Schema,
  onnx_mini_lm_l6_v2: onnxMiniLmL6V2Schema as Schema,
  open_clip: openClipSchema as Schema,
  openai: openaiSchema as Schema,
  roboflow: roboflowSchema as Schema,
  sentence_transformer: sentenceTransformerSchema as Schema,
  text2vec: text2vecSchema as Schema,
  transformers: transformersSchema as Schema,
  voyageai: voyageaiSchema as Schema,
};

/**
 * Load a JSON schema.
 *
 * @param schemaName Name of the schema file (without .json extension)
 * @returns The loaded schema as an object
 * @throws Error if the schema is not available
 */
export function loadSchema(schemaName: keyof typeof schemaMap): Schema {
  if (!schemaMap[schemaName]) {
    throw new Error(`Schema '${schemaName}' not found`);
  }

  return schemaMap[schemaName];
}

/**
 * Validate a configuration against a schema.
 *
 * @param config Configuration to validate
 * @param schemaName Name of the schema file (without .json extension)
 * @throws Error if the configuration does not match the schema
 */
export function validateConfigSchema(
  config: Record<string, any>,
  schemaName: keyof typeof schemaMap,
): void {
  const schema = loadSchema(schemaName);

  const validate = ajv.compile(schema);
  const valid = validate(config);

  if (!valid) {
    const errors = validate.errors || [];
    const errorPaths = errors
      .map((e) => `${e.instancePath || "/"}: ${e.message}`)
      .join(", ");
    throw new Error(
      `Config validation failed for schema '${schemaName}': ${errorPaths}`,
    );
  }
}

/**
 * Get the version of a schema.
 *
 * @param schemaName Name of the schema file (without .json extension)
 * @returns The schema version as a string
 * @throws Error if the schema file does not exist or is not valid JSON
 */
export function getSchemaVersion(schemaName: keyof typeof schemaMap): string {
  const schema = loadSchema(schemaName);
  return schema.version || "1.0.0";
}

/**
 * Get a list of all available schemas.
 *
 * @returns A list of schema names (without .json extension)
 */
export function getAvailableSchemas(): (keyof typeof schemaMap)[] {
  return Object.keys(schemaMap).filter((name) => name !== "base_schema") as (keyof typeof schemaMap)[];
}

/**
 * Get information about all available schemas.
 *
 * @returns A dictionary mapping schema names to information about the schema
 */
export function getSchemaInfo(): Record<
  string,
  { version: string; title: string; description: string }
> {
  const schemaInfo: Record<
    string,
    { version: string; title: string; description: string }
  > = {};

  for (const schemaName of getAvailableSchemas()) {
    try {
      const schema = schemaMap[schemaName];
      schemaInfo[schemaName] = {
        version: schema.version || "1.0.0",
        title: schema.title || "",
        description: schema.description || "",
      };
    } catch (error) {
      console.error(`Failed to load schema '${schemaName}':`, error);
    }
  }

  return schemaInfo;
}
