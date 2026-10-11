import { expect, test } from "@jest/globals";
import fs from "fs";
import path from "path";

test("does not ship testcontainers as a runtime dependency", () => {
  const packageJsonPath = path.resolve(__dirname, "package.json");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));

  expect(packageJson.dependencies?.testcontainers).toBeUndefined();
});
