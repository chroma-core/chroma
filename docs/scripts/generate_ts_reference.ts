#!/usr/bin/env bun
/**
 * Generate TypeScript SDK reference documentation for Chroma.
 *
 * Usage:
 *     bun run generate_ts_reference.ts --output reference/typescript/
 *
 * Writes client.mdx, collection.mdx, embedding-functions.mdx, search.mdx, and schema.mdx.
 * The file reference/typescript/where-filter.mdx is maintained by hand (TypeScript DSL only).
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
  items: Array<{ name: string; displayName?: string; sourceFile?: string }>;
  sourceFile?: string;
  sourceClass?: string;
  outputFile: string;
  showClassMethods?: boolean;
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
    const typeNode = param.getTypeNode();
    const countBefore = params.length;

    if (typeNode) {
      let typeText = typeNode
        .getText()
        .replace(/\s*=\s*(\{\s*\}|undefined|\[\s*\])\s*$/, "")
        .trim();

      if (typeText.includes("Partial<") || !typeText.includes("{")) {
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

      if (params.length === countBefore) {
        const isOptional = param.isOptional() || param.hasInitializer();
        params.push({
          name: paramName,
          type: formatType(param.getType()),
          description: getJSDocParamDescription(constructor, paramName),
          required: !isOptional,
        });
      }
    } else {
      const isOptional = param.isOptional() || param.hasInitializer();
      params.push({
        name: paramName,
        type: formatType(param.getType()),
        description: getJSDocParamDescription(constructor, paramName),
        required: !isOptional,
      });
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

function mdxEscape(text: string): string {
  return text.replace(/\\/g, "\\\\").replace(/\{/g, "\\{").replace(/\}/g, "\\}");
}

function attrType(type: string): string {
  const t = type.trim().replace(/^["']|["']$/g, "").replace(/"/g, "&quot;");
  return t;
}

function renderParam(p: Param): string {
  const attrs = [`path="${p.name}"`, `type="${attrType(p.type)}"`];
  if (p.required) attrs.push("required");

  if (p.description) {
    const desc = mdxEscape(p.description.trim());
    return `<ParamField ${attrs.join(" ")}>\n  ${desc}\n</ParamField>\n`;
  }
  return `<ParamField ${attrs.join(" ")} />\n`;
}

function renderMethod(method: MethodDoc, headingLevel: number = 3): string {
  const heading = "#".repeat(headingLevel);
  const lines: string[] = [`${heading} ${method.name}\n`];

  if (method.description) {
    lines.push(`${mdxEscape(method.description)}\n`);
  }

  lines.push(...method.params.map(renderParam));

  if (method.returns) {
    lines.push(`**Returns:** ${mdxEscape(method.returns)}\n`);
  }

  return lines.join("\n");
}

function renderClass(
  cls: ClassDoc,
  options: { fullMethods?: boolean; showMethods?: boolean; headingLevel?: number } = {}
): string {
  const { fullMethods = false, showMethods = true, headingLevel = 3 } = options;
  const heading = "#".repeat(headingLevel);
  const lines: string[] = [`${heading} ${cls.name}\n`];

  if (cls.description) {
    lines.push(`${mdxEscape(cls.description)}\n`);
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

  if (showMethods && cls.methods.length > 0) {
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
  params: Param[],
  headingLevel: number = 3
): string {
  const heading = "#".repeat(headingLevel);
  const lines: string[] = [`${heading} ${name}\n`];

  if (description) {
    lines.push(`${mdxEscape(description)}\n`);
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
      outputFile: "client",
      items: [
        { name: "ChromaClient", sourceFile: "chroma-client.ts" },
        { name: "CloudClient", sourceFile: "cloud-client.ts" },
        { name: "AdminClient", sourceFile: "admin-client.ts" },
      ],
    },
    {
      title: "Client Methods",
      renderMode: "method",
      sourceFile: "chroma-client.ts",
      sourceClass: "ChromaClient",
      outputFile: "client",
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
      outputFile: "client",
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
      outputFile: "collection",
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
      title: "Types",
      renderMode: "type",
      sourceFile: "types.ts",
      outputFile: "collection",
      items: [{ name: "GetResult" }, { name: "QueryResult" }],
    },
    {
      title: "Embedding Functions",
      renderMode: "class",
      sourceFile: "embedding-function.ts",
      outputFile: "embedding-functions",
      items: [
        { name: "EmbeddingFunction" },
        { name: "SparseEmbeddingFunction" },
      ],
    },
    {
      title: "Search",
      renderMode: "function",
      sourceFile: "execution/expression/search.ts",
      outputFile: "search",
      items: [{ name: "Search" }],
    },
    {
      title: "Select",
      renderMode: "function",
      sourceFile: "execution/expression/select.ts",
      outputFile: "search",
      items: [{ name: "Select" }],
    },
    {
      title: "Knn",
      renderMode: "type",
      sourceFile: "execution/expression/rank.ts",
      outputFile: "search",
      items: [{ name: "KnnOptions", displayName: "Knn" }],
    },
    {
      title: "Rrf",
      renderMode: "type",
      sourceFile: "execution/expression/rank.ts",
      outputFile: "search",
      items: [{ name: "RrfOptions", displayName: "Rrf" }],
    },
    {
      title: "Group By",
      renderMode: "function",
      sourceFile: "execution/expression/groupBy.ts",
      outputFile: "search",
      items: [
        { name: "GroupBy" },
        { name: "MinK" },
        { name: "MaxK" },
      ],
    },
    {
      title: "Group By",
      renderMode: "class",
      sourceFile: "execution/expression/limit.ts",
      outputFile: "search",
      items: [{ name: "Limit" }],
    },
    {
      title: "SearchResult",
      renderMode: "type",
      sourceFile: "execution/expression/searchResult.ts",
      outputFile: "search",
      items: [{ name: "SearchResult" }],
    },
    {
      title: "Schema",
      renderMode: "class",
      sourceFile: "schema.ts",
      outputFile: "schema",
      showClassMethods: false,
      items: [{ name: "Schema" }],
    },
    {
      title: "Index configs",
      renderMode: "class",
      sourceFile: "schema.ts",
      outputFile: "schema",
      showClassMethods: false,
      items: [
        { name: "FtsIndexConfig" },
        { name: "StringInvertedIndexConfig" },
        { name: "IntInvertedIndexConfig" },
        { name: "FloatInvertedIndexConfig" },
        { name: "BoolInvertedIndexConfig" },
        { name: "VectorIndexConfig" },
        { name: "SparseVectorIndexConfig" },
      ],
    },
  ];
}

function renderSection(
  project: Project,
  config: SectionConfig,
  sdkPath: string
): string {
  const lines: string[] = [];

  const singleItemName =
    config.items.length === 1
      ? (config.items[0].displayName || config.items[0].name)
      : undefined;
  const skipSectionHeading =
    singleItemName !== undefined && singleItemName === config.title;
  const headingLevel = skipSectionHeading ? 2 : 3;

  if (!skipSectionHeading) {
    lines.push(`## ${config.title}\n`);
  }

  const showClassMethods = config.showClassMethods !== false;

  for (const item of config.items) {
    const name = item.name;
    const displayName = item.displayName || name;
    const sourcePath = item.sourceFile ?? config.sourceFile;
    const sourceFile = sourcePath
      ? project.getSourceFile(path.join(sdkPath, sourcePath))
      : undefined;

    if (!sourceFile) {
      console.warn(`Source file not found: ${sourcePath}`);
      continue;
    }

    if (config.renderMode === "function") {
      const classDoc = extractClassOrInterface(sourceFile, name);
      const params = extractConstructorParams(sourceFile, name);
      lines.push(
        renderConstructorAsFunction(
          displayName,
          classDoc?.description,
          params,
          headingLevel
        )
      );
      lines.push("");
    } else if (config.renderMode === "method") {
      const classDecl = sourceFile.getClass(config.sourceClass!);
      const interfaceDecl = sourceFile.getInterface(config.sourceClass!);
      const node = classDecl || interfaceDecl;
      if (!node) {
        console.warn(`Class/interface not found: ${config.sourceClass}`);
        continue;
      }
      let method: MethodDeclaration | MethodSignature | undefined;
      if (classDecl) method = classDecl.getMethod(name);
      else if (interfaceDecl) method = interfaceDecl.getMethod(name);
      if (method) {
        const methodDoc = extractMethodFromDeclaration(method);
        lines.push(renderMethod(methodDoc, headingLevel));
        lines.push("");
      } else {
        console.warn(`Method not found: ${name} in ${config.sourceClass}`);
      }
    } else if (config.renderMode === "class" || config.renderMode === "class_full") {
      const classDoc = extractClassOrInterface(sourceFile, name);
      if (classDoc) {
        lines.push(
          renderClass(classDoc, {
            fullMethods: config.renderMode === "class_full",
            showMethods: showClassMethods,
            headingLevel,
          })
        );
        lines.push("");
      } else {
        console.warn(`Class/interface not found: ${name}`);
      }
    } else if (config.renderMode === "type") {
      const typeDoc = extractTypeAlias(sourceFile, name);
      if (typeDoc) {
        const heading = "#".repeat(headingLevel);
        lines.push(`${heading} ${displayName}\n`);
        if (typeDoc.description) {
          lines.push(`${mdxEscape(typeDoc.description)}\n`);
        }
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

const FILE_TITLES: Record<string, string> = {
  client: "Client",
  collection: "Collection",
  "embedding-functions": "Embedding Functions",
  search: "Search",
  schema: "Schema",
};

function getSectionsByFile(project: Project): Map<string, SectionConfig[]> {
  const sections = getDocumentationSections(project);
  const byFile = new Map<string, SectionConfig[]>();
  for (const config of sections) {
    const list = byFile.get(config.outputFile) ?? [];
    list.push(config);
    byFile.set(config.outputFile, list);
  }
  return byFile;
}

function generateDocumentationPerFile(sdkPath: string): Record<string, string> {
  const project = new Project({
    tsConfigFilePath: path.join(sdkPath, "../tsconfig.json"),
    skipAddingFilesFromTsConfig: true,
  });
  project.addSourceFilesAtPaths(path.join(sdkPath, "**/*.ts"));

  const byFile = getSectionsByFile(project);
  const out: Record<string, string> = {};

  for (const [fileStem, configs] of byFile) {
    const title = FILE_TITLES[fileStem] ?? fileStem.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    const lines: string[] = [
      "---",
      `title: "${title}"`,
      "---\n",
    ];
    for (let i = 0; i < configs.length; i++) {
      if (i > 0) lines.push("---\n");
      lines.push(renderSection(project, configs[i], sdkPath));
    }
    out[`${fileStem}.mdx`] = lines.join("\n");
  }

  return out;
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

  if (!values.output) {
    console.error("Usage: bun run generate_ts_reference.ts --output reference/typescript/");
    process.exit(1);
  }

  const scriptDir = path.dirname(Bun.main);
  const sdkPath = path.resolve(scriptDir, SDK_SOURCE_PATH);
  const mintlifyDir = path.resolve(scriptDir, "..", "mintlify");
  const outputDir = path.isAbsolute(values.output)
    ? values.output
    : path.resolve(mintlifyDir, values.output);
  const outDir = path.extname(outputDir) ? path.dirname(outputDir) : outputDir;

  const files = generateDocumentationPerFile(sdkPath);
  Bun.spawnSync(["mkdir", "-p", outDir]);

  for (const [filename, content] of Object.entries(files)) {
    const filePath = path.join(outDir, filename);
    Bun.write(filePath, content);
    console.log(`Generated: ${filePath}`);
  }
}

main();
