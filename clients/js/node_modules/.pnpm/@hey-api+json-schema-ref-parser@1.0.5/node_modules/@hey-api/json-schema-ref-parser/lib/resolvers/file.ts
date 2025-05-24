import fs from "fs";
import { ono } from "@jsdevtools/ono";
import * as url from "../util/url.js";
import { ResolverError } from "../util/errors.js";
import type { FileInfo } from "../types/index.js";

export const fileResolver = {
  handler: async ({
    file,
  }: {
    file: FileInfo;
  }): Promise<void> => {
    let path: string | undefined;

    try {
      path = url.toFileSystemPath(file.url);
    } catch (error: any) {
      throw new ResolverError(ono.uri(error, `Malformed URI: ${file.url}`), file.url);
    }

    try {
      const data = await fs.promises.readFile(path);
      file.data = data;
    } catch (error: any) {
      throw new ResolverError(ono(error, `Error opening file "${path}"`), path);
    }
  },
};
