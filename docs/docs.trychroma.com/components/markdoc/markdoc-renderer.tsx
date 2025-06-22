import fs from "fs";
import path from "path";
import React from "react";
import Markdoc from "@markdoc/markdoc";
import markdocConfig from "@/markdoc/config";
import { notFound } from "next/navigation";
import MarkdocPage from "@/components/markdoc/markdoc-page";
import SidebarToggle from "@/components/header/sidebar-toggle";
import { GitHubLogoIcon } from "@radix-ui/react-icons";
import Link from "next/link";
import { getAllPages, getPagePrevNext } from "@/lib/content";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import PageNav from "@/components/markdoc/page-nav";
import { TableOfContents } from "../table-of-contents";

const MarkdocRenderer: React.FC<{ slug: string[] }> = ({ slug }) => {
  const filePath = `${path.join(process.cwd(), "markdoc", "content", ...slug)}.md`;

  if (!fs.existsSync(filePath)) {
    notFound();
  }

  const source = fs.readFileSync(filePath, "utf-8");

  const ast = Markdoc.parse(source);


  // @ts-expect-error - This is a private function
  function extractToc(ast) {
    // @ts-expect-error - This is a private function
    const toc = [];

    // @ts-expect-error - This is a private function
    function traverse(node) {
      if (!node) return;

      if (node.type === "heading") {
        const title = node.children[0].children[0].attributes.content;
        if (!title) {
          console.warn(
            `Heading without content found in node: ${JSON.stringify(node)}`,
          );
        }
        const id =
          node.attributes.id ||
          title.toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, ""); // Generate an ID if missing

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
    }

    traverse(ast);
    // @ts-expect-error - This is a private function
    return toc;
  }

  // Extracts text recursively from children nodes
  // function extractText(node) {
  //   if (!node || !node.children) return "";

  //   return node.children
  //     .map((child) => {
  //       if (child.type === "text") return child.content || ""; // Direct text content
  //       if (child.children) return extractText(child); // Recursively extract from nested elements
  //       return "";
  //     })
  //     .join("")
  //     .trim();
  // }

  const toc = extractToc(ast);

  const content = Markdoc.transform(ast, markdocConfig);

  const output = Markdoc.renderers.react(content, React, {
    components: markdocConfig.components,
  });

  const GitHubLink = `https://github.com/chroma-core/chroma/tree/main/docs/docs.trychroma.com/markdoc/content/${slug.join("/")}.md`;

  const { prev, next } = getPagePrevNext(
    slug,
    getAllPages(sidebarConfig, slug[0]),
  );

  return (
    <MarkdocPage>
      <div className="flex max-w-6xl 2xl:max-w-7xl mx-auto">
        <div className="py-10 relative md:pr-10 marker:text-black dark:marker:text-gray-200 grow max-w-6xl w-full grow-4 prose dark:prose-invert ">
          <SidebarToggle path={slug} />
          {output}
          <div className="flex items-center justify-between mt-5">
            {prev ? (
              <PageNav slug={prev.slug || ""} name={prev.name} type="prev" />
            ) : (
              <div />
            )}
            {next ? (
              <PageNav slug={next.slug || ""} name={next.name} type="next" />
            ) : (
              <div />
            )}
          </div>
          <div className="flex items-center gap-2 mt-5 max-w-6xl">
            <GitHubLogoIcon className="w-5 h-5" />
            <Link href={GitHubLink}>Edit this page on GitHub</Link>
          </div>
        </div>
        <TableOfContents toc={toc} />
      </div>
    </MarkdocPage>
  );
};

export default MarkdocRenderer;
