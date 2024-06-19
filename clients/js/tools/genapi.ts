import "isomorphic-fetch";
import fs from "fs";
import { exec } from "node:child_process";
import { promisify } from "node:util";

const execPromise = promisify(exec);

const RUN_GENERATOR_SCRIPT = `
openapi-generator-plus -c config.yml

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' src/generated/runtime.ts
else
  sed -i -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' src/generated/runtime.ts
fi

# Add isomorphic-fetch dependency to runtime.ts
echo "import 'isomorphic-fetch';" >temp.txt
cat src/generated/runtime.ts >>temp.txt
mv temp.txt src/generated/runtime.ts
`;

// this type guard is to apply the minimal type to allow testing if an object
// has a field without allowing referencing fields that may not exist
function isObj(obj: unknown): obj is {} {
  return Boolean(obj && typeof obj === "object");
}

// this is to allow indexing into an object without knowing the field names
function isRecord(obj: unknown): obj is Record<string, unknown> {
  return Boolean(obj && typeof obj === "object");
}

const FIXES = [
  {
    matcher: (node: unknown): node is Record<string, unknown> =>
      isObj(node) &&
      "schema" in node &&
      isObj(node.schema) &&
      Object.keys(node.schema).length === 0,
    fix: (node: any) => {
      return {
        ...node,
        schema: {
          type: "object",
        },
      };
    },
  },
  {
    matcher: (node: unknown): node is Record<string, unknown> =>
      isObj(node) &&
      "items" in node &&
      isObj(node.items) &&
      Object.keys(node.items).length === 0,
    fix: (node: any) => {
      return {
        ...node,
        items: {
          type: "object",
        },
      };
    },
  },
  {
    matcher: (node: unknown): node is Record<string, unknown> =>
      isObj(node) && "title" in node && node.title === "Collection Name",
    fix: (node: any) => {
      return {
        ...node,
        type: "string",
      };
    },
  },
  {
    matcher: (node: unknown) =>
      isObj(node) &&
      "anyOf" in node &&
      Array.isArray(node.anyOf) &&
      node.anyOf.find((x: unknown) => isObj(x) && "const" in x),
    fix: (node: any) => {
      return {
        enum: node.anyOf.map((x: any) => x.const),
      };
    },
  },
];

function fixNode(node: any) {
  for (const fix of FIXES) {
    if (fix.matcher(node)) {
      node = fix.fix(node);
    }
  }

  if (isRecord(node)) {
    for (const key in node) {
      node[key] = fixNode(node[key]);
    }
  } else if (Array.isArray(node)) {
    node = node.map(fixNode);
  }

  return node;
}

async function main() {
  const resp = await fetch("http://localhost:8000/openapi.json");
  const openapi = await resp.json();
  const newApi = fixNode(openapi);
  fs.writeFileSync("./openapi.json", JSON.stringify(newApi, null, 2), "utf-8");

  // TODO: it would be nice not to have to shell out to the script
  await execPromise(RUN_GENERATOR_SCRIPT);

  fs.rmSync("./openapi.json");
}

main()
  .then(() => {
    console.log("done");
  })
  .catch((error) => {
    console.error(error);
  });
