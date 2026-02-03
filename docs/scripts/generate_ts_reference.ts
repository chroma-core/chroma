#!/usr/bin/env bun
/**
 * Generate TypeScript SDK reference documentation for Chroma.
 *
 * Usage:
 *     bun run generate_ts_reference.ts --output mintlify/reference/typescript/index.mdx
 *
 * This script introspects the chromadb TypeScript package and generates MDX documentation
 * with ParamField components for Mintlify.
 */

import { Project, SourceFile, ClassDeclaration, InterfaceDeclaration, MethodSignature, MethodDeclaration, PropertySignature, PropertyDeclaration, JSDoc, Type, Node, ParameterDeclaration, TypeAliasDeclaration } from "ts-morph";
import * as path from "path";
import { parseArgs } from "util";

// =============================================================================
// Configuration
// =============================================================================

const SDK_SOURCE_PATH = "../../clients/new-js/packages/chromadb/src";

const TYPE_SIMPLIFICATIONS: Record<string, string> = {
  "number[][]": "Embeddings",
  "number[]": "Embedding",
  "Record<string, boolean | number | string | SparseVector | null>": "Metadata",
  "(ReadLevel)[keyof ReadLevel]": '"index_and_wal" | "index_only"',
  "(typeof ReadLevel)[keyof typeof ReadLevel]": '"index_and_wal" | "index_only"',
};

// =============================================================================
// Data Model
// =============================================================================

interface Param {
  name: string;
  type: string;
  description?: string;
  required: boolean;
}

interface MethodDoc {
  name: string;
  description?: string;
  params: Param[];
  returns?: string;
  isAsync: boolean;
}

interface PropertyDoc {
  name: string;
  type: string;
  description?: string;
}

interface ClassDoc {
  name: string;
  description?: string;
  properties: PropertyDoc[];
  methods: MethodDoc[];
}

interface SectionConfig {
  title: string;
  renderMode: "function" | "method" | "class" | "class_full" | "type";
  items: Array<{ name: string; displayName?: string }>;
  sourceFile?: string;
  sourceClass?: string;
}

// =============================================================================
// Type Formatting
// =============================================================================

function simplifyType(typeStr: string): string {
  for (const [pattern, replacement] of Object.entries(TYPE_SIMPLIFICATIONS)) {
    if (typeStr.includes(pattern)) {
      typeStr = typeStr.replace(pattern, replacement);
    }
  }
  return typeStr;
}

function formatType(type: Type | string, preserveNewlines: boolean = false): string {
  const typeStr = typeof type === "string" ? type : type.getText();

  let simplified = typeStr
    .replace(/import\([^)]+\)\./g, "")
    .replace(/typeof /g, "");

  // Normalize whitespace unless preserving newlines
  if (!preserveNewlines) {
    simplified = simplified.replace(/\s+/g, " ").trim();
  }

  // Remove leading | from union types (TypeScript formatting artifact)
  simplified = simplified.replace(/^\|\s*/, "");

  simplified = simplifyType(simplified);

  if (simplified.length > 80) {
    if (simplified.includes("number[][]")) return "Embeddings";
    if (simplified.includes("number[]")) return "Embedding";
    if (simplified.includes("Record<string,")) return "Metadata";
  }

  return simplified;
}

// =============================================================================
// JSDoc Extraction
// =============================================================================

function getJSDocDescription(node: Node): string | undefined {
  const jsDocs = Node.isJSDocable(node) ? node.getJsDocs() : [];
  if (jsDocs.length === 0) return undefined;

  const jsDoc = jsDocs[0];
  let description = jsDoc.getDescription();
  if (description) {
    // Clean up multi-line descriptions - remove asterisks and normalize whitespace
    description = description.replace(/\n\s*\*\s*/g, "\n").trim();
  }
  return description || undefined;
}

function getJSDocParamDescription(node: Node, paramName: string): string | undefined {
  const jsDocs = Node.isJSDocable(node) ? node.getJsDocs() : [];
  if (jsDocs.length === 0) return undefined;

  const jsDoc = jsDocs[0];
  const tags = jsDoc.getTags();

  for (const tag of tags) {
    if (tag.getTagName() === "param") {
      const text = tag.getText();
      const match = text.match(/@param\s+(?:\{[^}]+\}\s+)?(\w+)\s*-?\s*(.*)/s);
      if (match && match[1] === paramName) {
        // Clean up the description - remove asterisks and extra whitespace from multi-line comments
        let desc = match[2]?.trim() || undefined;
        if (desc) {
          desc = desc.replace(/\n\s*\*\s*/g, " ").replace(/\s+/g, " ").trim();
        }
        return desc || undefined;
      }
    }
  }
  return undefined;
}

function getJSDocReturns(node: Node): string | undefined {
  const jsDocs = Node.isJSDocable(node) ? node.getJsDocs() : [];
  if (jsDocs.length === 0) return undefined;

  const jsDoc = jsDocs[0];
  const tags = jsDoc.getTags();

  for (const tag of tags) {
    if (tag.getTagName() === "returns" || tag.getTagName() === "return") {
      const text = tag.getComment();
      return typeof text === "string" ? text.trim() : text?.toString().trim();
    }
  }
  return undefined;
}

// =============================================================================
// Extraction
// =============================================================================

function extractMethodFromDeclaration(method: MethodDeclaration | MethodSignature): MethodDoc {
  const name = method.getName();
  const description = getJSDocDescription(method);
  const returns = getJSDocReturns(method);

  const params: Param[] = [];
  const parameters = method.getParameters();

  for (const param of parameters) {
    const paramName = param.getName();
    if (paramName === "this") continue;

    const paramType = param.getType();
    const isOptional = param.isOptional() || param.hasInitializer();
    const paramDesc = getJSDocParamDescription(method, paramName);

    // Handle destructured parameters (objects)
    if (paramName.startsWith("{") || param.getTypeNode()?.getText().includes("{")) {
      const typeNode = param.getTypeNode();
      if (typeNode) {
        let typeText = typeNode.getText();
        // Strip JSDoc comments from the type text before parsing
        typeText = typeText.replace(/\/\*\*[\s\S]*?\*\//g, "");
        // Parse inline object type - handle both simple and complex types
        // Match property names followed by optional ? and type annotation
        const propMatches = typeText.matchAll(/(\w+)(\?)?:\s*([^;,}]+?)(?=[;,}]|$)/g);
        for (const match of propMatches) {
          const propName = match[1];
          // Skip if it looks like a JSDoc artifact or keyword
          if (propName === "default" || propName.startsWith("*")) continue;

          const propOptional = !!match[2];
          let propType = match[3].trim();
          // Clean up any trailing comment markers
          propType = propType.replace(/\s*\/\*.*$/, "").trim();

          params.push({
            name: propName,
            type: formatType(propType),
            description: getJSDocParamDescription(method, propName),
            required: !propOptional,
          });
        }
      }
      continue;
    }

    params.push({
      name: paramName,
      type: formatType(paramType),
      description: paramDesc,
      required: !isOptional,
    });
  }

  const isAsync = Node.isMethodDeclaration(method)
    ? method.isAsync() || method.getReturnType().getText().startsWith("Promise")
    : method.getReturnType().getText().startsWith("Promise");

  return {
    name,
    description,
    params,
    returns,
    isAsync,
  };
}

function extractClassOrInterface(
  sourceFile: SourceFile,
  name: string
): ClassDoc | undefined {
  const classDecl = sourceFile.getClass(name);
  const interfaceDecl = sourceFile.getInterface(name);

  const node = classDecl || interfaceDecl;
  if (!node) return undefined;

  const description = getJSDocDescription(node);

  const properties: PropertyDoc[] = [];
  const methods: MethodDoc[] = [];

  if (classDecl) {
    // Extract properties from class
    for (const prop of classDecl.getProperties()) {
      if (prop.hasModifier("private") || prop.getName().startsWith("_")) continue;

      properties.push({
        name: prop.getName(),
        type: formatType(prop.getType()),
        description: getJSDocDescription(prop),
      });
    }

    // Extract methods from class
    for (const method of classDecl.getMethods()) {
      if (method.hasModifier("private") || method.getName().startsWith("_")) continue;

      methods.push(extractMethodFromDeclaration(method));
    }
  } else if (interfaceDecl) {
    // Extract properties from interface
    for (const prop of interfaceDecl.getProperties()) {
      if (prop.getName().startsWith("_")) continue;

      properties.push({
        name: prop.getName(),
        type: formatType(prop.getType()),
        description: getJSDocDescription(prop),
      });
    }

    // Extract methods from interface
    for (const method of interfaceDecl.getMethods()) {
      if (method.getName().startsWith("_")) continue;

      methods.push(extractMethodFromDeclaration(method));
    }
  }

  return {
    name,
    description,
    properties,
    methods: methods.sort((a, b) => a.name.localeCompare(b.name)),
  };
}

function extractConstructorParams(
  sourceFile: SourceFile,
  className: string
): Param[] {
  const classDecl = sourceFile.getClass(className);
  if (!classDecl) return [];

  const constructor = classDecl.getConstructors()[0];
  if (!constructor) return [];

  const params: Param[] = [];
  const parameters = constructor.getParameters();

  for (const param of parameters) {
    const paramName = param.getName();

    // Handle object parameter (args object)
    const typeNode = param.getTypeNode();
    if (typeNode) {
      const typeText = typeNode.getText();

      // Check if it's a reference to an interface
      if (typeText.includes("Partial<") || !typeText.includes("{")) {
        // Try to resolve the interface
        const interfaceName = typeText.replace("Partial<", "").replace(">", "").trim();
        const interfaceDecl = sourceFile.getInterface(interfaceName);

        if (interfaceDecl) {
          for (const prop of interfaceDecl.getProperties()) {
            const propName = prop.getName();
            const isOptional = prop.hasQuestionToken() || typeText.includes("Partial<");

            params.push({
              name: propName,
              type: formatType(prop.getType()),
              description: getJSDocDescription(prop),
              required: !isOptional,
            });
          }
          continue;
        }
      }

      // Parse inline object type
      const propMatches = typeText.matchAll(/(\w+)(\?)?:\s*([^;,}]+)/g);
      for (const match of propMatches) {
        const propName = match[1];
        const propOptional = !!match[2];
        const propType = match[3].trim();

        params.push({
          name: propName,
          type: formatType(propType),
          description: getJSDocParamDescription(constructor, propName),
          required: !propOptional,
        });
      }
    }
  }

  return params;
}

interface TypeDoc {
  name: string;
  description?: string;
  typeDefinition?: string;
  properties: PropertyDoc[];
  methods: MethodDoc[];
}

function extractTypeAlias(
  sourceFile: SourceFile,
  name: string
): TypeDoc | undefined {
  const typeAlias = sourceFile.getTypeAlias(name);
  const enumDecl = sourceFile.getEnum(name);
  const classDecl = sourceFile.getClass(name);
  const interfaceDecl = sourceFile.getInterface(name);

  if (typeAlias) {
    const description = getJSDocDescription(typeAlias);
    const typeNode = typeAlias.getTypeNode();
    const typeText = typeNode ? typeNode.getText() : typeAlias.getType().getText();

    return {
      name,
      description,
      typeDefinition: formatType(typeText),
      properties: [],
      methods: [],
    };
  }

  if (enumDecl) {
    const description = getJSDocDescription(enumDecl);
    const members = enumDecl.getMembers().map(m => `"${m.getName()}"`);

    return {
      name,
      description,
      typeDefinition: members.join(" | "),
      properties: [],
      methods: [],
    };
  }

  if (classDecl || interfaceDecl) {
    return extractClassOrInterface(sourceFile, name);
  }

  // Try to find as a const (like ReadLevel)
  const varDecl = sourceFile.getVariableDeclaration(name);
  if (varDecl) {
    const description = getJSDocDescription(varDecl);
    const typeText = varDecl.getType().getText();

    return {
      name,
      description,
      typeDefinition: formatType(typeText),
      properties: [],
      methods: [],
    };
  }

  return undefined;
}

// =============================================================================
// MDX Rendering
// =============================================================================

function renderParam(p: Param): string {
  const attrs = [`path="${p.name}"`, `type="${p.type}"`];
  if (p.required) attrs.push("required");

  if (p.description) {
    return `<ParamField ${attrs.join(" ")}>\n  ${p.description.trim()}\n</ParamField>\n`;
  }
  return `<ParamField ${attrs.join(" ")} />\n`;
}

function renderMethod(method: MethodDoc, headingLevel: number = 3): string {
  const heading = "#".repeat(headingLevel);
  const lines: string[] = [`${heading} ${method.name}\n`];

  if (method.description) {
    lines.push(`${method.description}\n`);
  }

  lines.push(...method.params.map(renderParam));

  if (method.returns) {
    lines.push(`**Returns:** ${method.returns}\n`);
  }

  return lines.join("\n");
}

function renderClass(cls: ClassDoc, fullMethods: boolean = false): string {
  const lines: string[] = [`### ${cls.name}\n`];

  if (cls.description) {
    lines.push(`${cls.description}\n`);
  }

  if (cls.properties.length > 0) {
    lines.push('<span class="text-sm">Properties</span>\n');
    for (const prop of cls.properties) {
      lines.push(renderParam({
        name: prop.name,
        type: prop.type,
        description: prop.description,
        required: false,
      }));
    }
  }

  if (cls.methods.length > 0) {
    if (fullMethods) {
      lines.push('<span class="text-sm">Methods</span>\n');
      for (const method of cls.methods) {
        lines.push(renderMethod(method, 4));
      }
    } else {
      lines.push('\n<span class="text-sm">Methods</span>\n');
      lines.push(cls.methods.map(m => `\`${m.name}()\``).join(", ") + "\n");
    }
  }

  return lines.join("\n");
}

function renderConstructorAsFunction(
  name: string,
  description: string | undefined,
  params: Param[]
): string {
  const lines: string[] = [`### ${name}\n`];

  if (description) {
    lines.push(`${description}\n`);
  }

  lines.push(...params.map(renderParam));

  return lines.join("\n");
}

// =============================================================================
// Document Generation
// =============================================================================

function getDocumentationSections(project: Project): SectionConfig[] {
  return [
    {
      title: "Clients",
      renderMode: "function",
      sourceFile: "chroma-client.ts",
      items: [
        { name: "ChromaClient" },
      ],
    },
    {
      title: "Clients",
      renderMode: "function",
      sourceFile: "cloud-client.ts",
      items: [
        { name: "CloudClient" },
      ],
    },
    {
      title: "Clients",
      renderMode: "function",
      sourceFile: "admin-client.ts",
      items: [
        { name: "AdminClient" },
      ],
    },
    {
      title: "Client Methods",
      renderMode: "method",
      sourceFile: "chroma-client.ts",
      sourceClass: "ChromaClient",
      items: [
        { name: "heartbeat" },
        { name: "listCollections" },
        { name: "countCollections" },
        { name: "createCollection" },
        { name: "getCollection" },
        { name: "getOrCreateCollection" },
        { name: "deleteCollection" },
        { name: "reset" },
        { name: "version" },
      ],
    },
    {
      title: "Admin Client Methods",
      renderMode: "method",
      sourceFile: "admin-client.ts",
      sourceClass: "AdminClient",
      items: [
        { name: "createTenant" },
        { name: "getTenant" },
        { name: "createDatabase" },
        { name: "getDatabase" },
        { name: "deleteDatabase" },
        { name: "listDatabases" },
      ],
    },
    {
      title: "Collection Methods",
      renderMode: "method",
      sourceFile: "collection.ts",
      sourceClass: "Collection",
      items: [
        { name: "count" },
        { name: "add" },
        { name: "get" },
        { name: "peek" },
        { name: "query" },
        { name: "modify" },
        { name: "update" },
        { name: "upsert" },
        { name: "delete" },
        { name: "search" },
      ],
    },
    {
      title: "Embedding Functions",
      renderMode: "class",
      sourceFile: "embedding-function.ts",
      items: [
        { name: "EmbeddingFunction" },
        { name: "SparseEmbeddingFunction" },
      ],
    },
    {
      title: "Types",
      renderMode: "type",
      sourceFile: "types.ts",
      items: [
        { name: "CollectionMetadata" },
        { name: "Metadata" },
        { name: "Where" },
        { name: "WhereDocument" },
        { name: "GetResult" },
        { name: "QueryResult" },
        { name: "ReadLevel" },
        { name: "IncludeEnum" },
      ],
    },
    {
      title: "Types",
      renderMode: "type",
      sourceFile: "execution/expression/search.ts",
      items: [
        { name: "Search" },
      ],
    },
    {
      title: "Types",
      renderMode: "type",
      sourceFile: "execution/expression/select.ts",
      items: [
        { name: "Select" },
      ],
    },
    {
      title: "Types",
      renderMode: "type",
      sourceFile: "execution/expression/rank.ts",
      items: [
        { name: "KnnOptions", displayName: "Knn" },
        { name: "RrfOptions", displayName: "Rrf" },
      ],
    },
    {
      title: "Types",
      renderMode: "type",
      sourceFile: "execution/expression/searchResult.ts",
      items: [
        { name: "SearchResult" },
      ],
    },
    {
      title: "Types",
      renderMode: "type",
      sourceFile: "schema.ts",
      items: [
        { name: "Schema" },
      ],
    },
  ];
}

function renderSection(
  project: Project,
  config: SectionConfig,
  sdkPath: string,
  isFirstOfTitle: boolean
): string {
  const lines: string[] = [];

  if (isFirstOfTitle) {
    lines.push(`## ${config.title}\n`);
  }

  const sourceFile = config.sourceFile
    ? project.getSourceFile(path.join(sdkPath, config.sourceFile))
    : undefined;

  if (!sourceFile) {
    console.warn(`Source file not found: ${config.sourceFile}`);
    return lines.join("\n");
  }

  for (const item of config.items) {
    const name = item.name;
    const displayName = item.displayName || name;

    if (config.renderMode === "function") {
      // Render class constructor as a function
      const classDoc = extractClassOrInterface(sourceFile, name);
      const params = extractConstructorParams(sourceFile, name);

      lines.push(renderConstructorAsFunction(
        displayName,
        classDoc?.description,
        params
      ));
      lines.push("");
    } else if (config.renderMode === "method") {
      // Render specific methods from a class/interface
      const classDecl = sourceFile.getClass(config.sourceClass!);
      const interfaceDecl = sourceFile.getInterface(config.sourceClass!);

      const node = classDecl || interfaceDecl;
      if (!node) {
        console.warn(`Class/interface not found: ${config.sourceClass}`);
        continue;
      }

      let method: MethodDeclaration | MethodSignature | undefined;
      if (classDecl) {
        method = classDecl.getMethod(name);
      } else if (interfaceDecl) {
        method = interfaceDecl.getMethod(name);
      }

      if (method) {
        const methodDoc = extractMethodFromDeclaration(method);
        lines.push(renderMethod(methodDoc));
        lines.push("");
      } else {
        console.warn(`Method not found: ${name} in ${config.sourceClass}`);
      }
    } else if (config.renderMode === "class" || config.renderMode === "class_full") {
      const classDoc = extractClassOrInterface(sourceFile, name);
      if (classDoc) {
        lines.push(renderClass(classDoc, config.renderMode === "class_full"));
        lines.push("");
      } else {
        console.warn(`Class/interface not found: ${name}`);
      }
    } else if (config.renderMode === "type") {
      const typeDoc = extractTypeAlias(sourceFile, name);
      if (typeDoc) {
        lines.push(`### ${displayName}\n`);
        if (typeDoc.description) {
          lines.push(`${typeDoc.description}\n`);
        }
        // Show type definition if no properties (for type aliases)
        if (typeDoc.properties.length === 0 && (typeDoc as TypeDoc).typeDefinition) {
          lines.push(`\`${(typeDoc as TypeDoc).typeDefinition}\`\n`);
        }
        if (typeDoc.properties.length > 0) {
          lines.push('<span class="text-sm">Properties</span>\n');
          for (const prop of typeDoc.properties) {
            lines.push(renderParam({
              name: prop.name,
              type: prop.type,
              description: prop.description,
              required: false,
            }));
          }
        }
        lines.push("");
      } else {
        console.warn(`Type not found: ${name}`);
      }
    }
  }

  return lines.join("\n");
}

const INSTALLATION_SECTION = `## Installation

<CodeGroup>
\`\`\`bash npm
npm install chromadb
\`\`\`
\`\`\`bash pnpm
pnpm add chromadb
\`\`\`
\`\`\`bash bun
bun add chromadb
\`\`\`
\`\`\`bash yarn
yarn add chromadb
\`\`\`
</CodeGroup>

The TypeScript SDK also has multiple extensions for popular embedding providers. See the full list on the [integrations page](/integrations/chroma-integrations).
`;

function generateDocumentation(sdkPath: string): string {
  const project = new Project({
    tsConfigFilePath: path.join(sdkPath, "../tsconfig.json"),
    skipAddingFilesFromTsConfig: true,
  });

  // Add source files
  project.addSourceFilesAtPaths(path.join(sdkPath, "**/*.ts"));

  const sections = getDocumentationSections(project);

  const lines: string[] = [
    "---",
    'title: "TypeScript Reference"',
    "---\n",
    INSTALLATION_SECTION,
    "---\n",
  ];

  let lastTitle = "";
  for (let i = 0; i < sections.length; i++) {
    const section = sections[i];
    const isFirstOfTitle = section.title !== lastTitle;

    if (isFirstOfTitle && lastTitle !== "") {
      lines.push("---\n");
    }

    lines.push(renderSection(project, section, sdkPath, isFirstOfTitle));
    lastTitle = section.title;
  }

  return lines.join("\n");
}

// =============================================================================
// CLI
// =============================================================================

function main() {
  const { values } = parseArgs({
    args: Bun.argv.slice(2),
    options: {
      output: {
        type: "string",
        short: "o",
      },
    },
    strict: true,
    allowPositionals: false,
  });

  const scriptDir = path.dirname(Bun.main);
  const sdkPath = path.resolve(scriptDir, SDK_SOURCE_PATH);

  const content = generateDocumentation(sdkPath);

  if (values.output) {
    // Resolve output path relative to the mintlify directory (parent of scripts)
    const mintlifyDir = path.resolve(scriptDir, "..", "mintlify");
    const outputPath = path.isAbsolute(values.output)
      ? values.output
      : path.resolve(mintlifyDir, values.output);

    const dir = path.dirname(outputPath);
    Bun.spawnSync(["mkdir", "-p", dir]);

    Bun.write(outputPath, content);
    console.log(`Generated: ${outputPath}`);
  } else {
    console.log(content);
  }
}

main();
