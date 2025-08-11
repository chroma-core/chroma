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
import TableOfContents from "@/components/markdoc/table-of-contents";
import AskAI from "@/components/markdoc/ask-ai";

const MarkdocRenderer: React.FC<{ slug: string[] }> = ({ slug }) => {
  const filePath = `${path.join(process.cwd(), "markdoc", "content", ...slug)}.md`;
  const txtFilePath = `${path.join(process.cwd(), "public", `llms-${slug.join("-")}`)}.txt`;
  console.log(txtFilePath);

  if (!fs.existsSync(filePath)) {
    notFound();
  }

  const source = fs.readFileSync(filePath, "utf-8");
  const txtContentExists = fs.existsSync(txtFilePath);
  const txtContent = txtContentExists
    ? fs.readFileSync(txtFilePath, "utf-8")
    : undefined;

  const ast = Markdoc.parse(source);

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
      <div className="relative flex max-w-6xl 2xl:max-w-7xl mx-auto">
        <div className="min-w-0 py-10 relative md:pr-10 marker:text-black dark:marker:text-gray-200 grow max-w-6xl w-full grow-4 prose dark:prose-invert ">
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
        <div className="absolute top-9 right-0 xl:hidden">
          <AskAI content={txtContent} />
        </div>
        <div className="sticky top-0 h-full py-5 space-y-4 hidden xl:block w-[250px]">
          <AskAI content={txtContent} />
          <TableOfContents ast={ast} />
        </div>
      </div>
    </MarkdocPage>
  );
};

export default MarkdocRenderer;
