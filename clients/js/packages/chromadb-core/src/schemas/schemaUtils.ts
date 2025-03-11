import * as path from "path";
import * as fs from "fs";
import Ajv from "ajv";

// Path to the schemas directory (relative to the project root)
const SCHEMAS_DIR = path.join(
  process.cwd(),
  "..",
  "..",
  "..",
  "..",
  "schemas",
  "embedding_functions",
);

// Cache for loaded schemas
const cachedSchemas: Record<string, any> = {};

/**
 * Load a JSON schema from the schemas directory.
 *
 * @param schemaName Name of the schema file (without .json extension)
 * @returns The loaded schema as an object
 * @throws Error if the schema file does not exist or is not valid JSON
 */
export function loadSchema(schemaName: string): Record<string, any> {
  if (cachedSchemas[schemaName]) {
    return cachedSchemas[schemaName];
  }

  try {
    const schemaPath = path.join(SCHEMAS_DIR, `${schemaName}.json`);
    const schemaContent = fs.readFileSync(schemaPath, "utf8");
    const schema = JSON.parse(schemaContent);
    cachedSchemas[schemaName] = schema;
    return schema;
  } catch (error) {
    if (error instanceof Error) {
      throw new Error(
        `Failed to load schema '${schemaName}': ${error.message}`,
      );
    }
    throw error;
  }
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
  schemaName: string,
): void {
  const schema = loadSchema(schemaName);

  // Create a copy of the schema without the version field
  const { version, ...schemaWithoutVersion } = schema;

  const ajv = new Ajv({
    strict: false, // Allow unknown keywords
    allErrors: true,
  });
  const validate = ajv.compile(schemaWithoutVersion);
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
export function getSchemaVersion(schemaName: string): string {
  const schema = loadSchema(schemaName);
  return schema.version || "1.0.0";
}

/**
 * Get a list of all available schemas.
 *
 * @returns A list of schema names (without .json extension)
 */
export function getAvailableSchemas(): string[] {
  try {
    return fs
      .readdirSync(SCHEMAS_DIR)
      .filter(
        (filename) =>
          filename.endsWith(".json") && filename !== "base_schema.json",
      )
      .map((filename) => filename.slice(0, -5)); // Remove .json extension
  } catch (error) {
    console.error("Failed to read schemas directory:", error);
    return [];
  }
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
      const schema = loadSchema(schemaName);
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
