import { createRequire } from "module";
const require = createRequire(import.meta.url);

let binding: any;

if (process.platform === "darwin") {
  if (process.arch === "arm64") {
    binding = require("chromadb-js-bindings-darwin-arm64");
  } else if (process.arch === "x64") {
    binding = require("chromadb-js-bindings-darwin-x64");
  } else {
    throw new Error(`Unsupported architecture on macOS: ${process.arch}`);
  }
} else if (process.platform === "linux") {
  if (process.arch === "arm64") {
    binding = require("chromadb-js-bindings-linux-arm64-gnu");
  } else if (process.arch === "x64") {
    binding = require("chromadb-js-bindings-linux-x64-gnu");
  } else {
    throw new Error(`Unsupported architecture on Linux: ${process.arch}`);
  }
} else if (process.platform === "win32") {
  if (process.arch === "arm64") {
    binding = require("chromadb-js-bindings-win32-arm64-msvc");
  } else {
    throw new Error(
      `Unsupported Windows architecture: ${process.arch}. Only ARM64 is supported.`,
    );
  }
} else {
  throw new Error(`Unsupported platform: ${process.platform}`);
}

export default binding;
