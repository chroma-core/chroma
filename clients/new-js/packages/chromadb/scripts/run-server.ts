#!/usr/bin/env node
import binding from "../src/bindings.js";

process.on("SIGTERM", () => {
  console.log("Server shutting down...");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("Server shutting down...");
  process.exit(0);
});

binding.cli(["chroma", "run"]);
