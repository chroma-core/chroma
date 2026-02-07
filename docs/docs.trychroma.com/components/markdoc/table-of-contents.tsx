import { TextIcon } from "lucide-react";
import React from "react";
import { Node } from "@markdoc/markdoc";

export interface PageSection {
  level: number;
  title: string;
  id: string;
}

const extractToc = (ast: Node) => {
  const toc: PageSection[] = [];

  const traverse = (node: Node) => {
    if (!node) return;

    if (node.type === "heading") {
      const title = node.children[0].children[0].attributes.content;
      const id =
        node.attributes.id ||
        title
          .toLowerCase()
          .replace(/\s+/g, "-")
          .replace(/[^a-z0-9-]/g, ""); // Generate an ID if missing

      toc.push({
        level: node.attributes.level, // Heading level (1, 2, 3...)
        title: title.trim(),
        id: id.trim(),
      });
    }

    // Recursively traverse children
    if (node.children) {
      for (const child of node.children) {
        traverse(child);
      }
    }
  };

  traverse(ast);

  // Normalize levels by constructing a hierarchical structure
  if (toc.length === 0) return toc;

  // Get unique levels and sort them
  const uniqueLevels = [...new Set(toc.map((item) => item.level))].sort(
    (a, b) => a - b,
  );

  // Create a mapping from original level to normalized level
  const levelMap = new Map<number, number>();
  uniqueLevels.forEach((level, index) => {
    levelMap.set(level, index);
  });

  // Apply normalized levels
  return toc.map((item) => ({
    ...item,
    level: levelMap.get(item.level) || 0,
  }));
};

const TableOfContents: React.FC<{ ast: Node }> = ({ ast }) => {
  const toc = extractToc(ast);
  return (
    <div className="overflow-y-auto">
      <div className="flex flex-row items-center font-bold">
        <TextIcon className="h-5 w-5 pr-1" />
        On this page
      </div>
      <nav>
        {toc.map((item) => {
          let padding;
          switch (item.level) {
            case 0:
              padding = "pl-0";
              break;
            case 1:
              padding = "pl-3";
              break;
            case 2:
              padding = "pl-6";
              break;
            case 3:
              padding = "pl-9";
              break;
            case 4:
              padding = "pl-12";
              break;
            case 5:
              padding = "pl-15";
              break;
            default:
              padding = "pl-15";
              break;
          }

          return (
            <div key={item.id} className={`mt-1 ${padding}`}>
              <a
                href={`#${item.id}`}
                className={`text-gray-700 dark:text-gray-200 text-sm font-normal no-underline transition-all hover:text-blue-500
                    `}
              >
                {item.title}
              </a>
            </div>
          );
        })}
      </nav>
    </div>
  );
};

export default TableOfContents;
