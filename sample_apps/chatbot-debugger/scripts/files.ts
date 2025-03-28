import * as fs from "fs";
import * as path from "path";
import { SUPPORTED_FILE_EXTENSIONS } from "@/scripts/chunking";
import { Result } from "@/lib/types";

const IGNORE_DIRECTORIES: string[] = ["node_modules", "venv"];

export interface LocalFile {
  name: string;
  path: string;
  fullPath: string;
  type: "file" | "dir";
  content?: string;
}

export const getAllFiles = (
  rootPath: string,
  currentPath: string = "",
  allowedExtensions: string[] = [],
  allowedDirectories: string[] = [],
): Result<LocalFile[], Error> => {
  const fullPath = path.join(rootPath, currentPath);
  let items;
  try {
    items = fs.readdirSync(fullPath, { withFileTypes: true });
  } catch {
    return {
      ok: false,
      error: new Error(`Failed to scan files from ${rootPath}`),
    };
  }

  let allFiles: LocalFile[] = [];

  for (const item of items) {
    if (item.name.startsWith(".")) {
      continue;
    }

    const itemPath = path.join(currentPath, item.name);
    if (IGNORE_DIRECTORIES.some((ignore) => itemPath.includes(ignore))) {
      continue;
    }

    if (item.isDirectory()) {
      const isAllowedDirectory =
        allowedDirectories.length === 0 ||
        allowedDirectories.some((dir) => {
          return (
            itemPath === dir ||
            itemPath.startsWith(`${dir}/`) ||
            dir.startsWith(`${itemPath}/`)
          );
        });

      if (isAllowedDirectory) {
        const filesInDir = getAllFiles(
          rootPath,
          itemPath,
          allowedExtensions,
          allowedDirectories,
        );

        if (!filesInDir.ok) {
          return filesInDir;
        }

        allFiles = [...allFiles, ...filesInDir.value];
      }
    } else if (item.isFile()) {
      const isInAllowedDirectory =
        allowedDirectories.length === 0 ||
        allowedDirectories.some((dir) => {
          return itemPath === dir || itemPath.startsWith(`${dir}/`);
        });

      const fileExtension = `.${item.name.split(".").pop() || ""}`;

      const hasAllowedExtension =
        SUPPORTED_FILE_EXTENSIONS.includes(fileExtension) &&
        (allowedExtensions.length === 0 ||
          allowedExtensions.some((ext) => ext === fileExtension));

      if (isInAllowedDirectory && hasAllowedExtension) {
        allFiles.push({
          name: item.name,
          path: itemPath,
          fullPath: path.join(rootPath, itemPath),
          type: "file",
        });
      }
    }
  }

  return { ok: true, value: allFiles };
};

export const readFileContent = (file: LocalFile): Result<string, Error> => {
  try {
    return { ok: true, value: fs.readFileSync(file.fullPath, "utf8") };
  } catch {
    return {
      ok: false,
      error: new Error(`Error reading content for ${file.path}:`),
    };
  }
};
