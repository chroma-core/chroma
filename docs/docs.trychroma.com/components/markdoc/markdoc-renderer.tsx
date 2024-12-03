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

const MarkdocRenderer: React.FC<{ slug: string[] }> = ({ slug }) => {
  const filePath = `${path.join(process.cwd(), "markdoc", "content", ...slug)}.md`;

  if (!fs.existsSync(filePath)) {
    notFound();
  }

  const source = fs.readFileSync(filePath, "utf-8");

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
      <div className="relative w-full h-full marker:text-black dark:marker:text-gray-200">
        <SidebarToggle path={slug} />
        {output}
        <div className="flex items-center justify-between mt-5">
          {prev ? (
            <PageNav path={prev.path || ""} name={prev.name} type="prev" />
          ) : (
            <div />
          )}
          {next ? (
            <PageNav path={next.path || ""} name={next.name} type="next" />
          ) : (
            <div />
          )}
        </div>
        <div className="flex items-center gap-2 mt-5">
          <GitHubLogoIcon className="w-5 h-5" />
          <Link href={GitHubLink}>Edit this page on GitHub</Link>
        </div>
      </div>
    </MarkdocPage>
  );
};

export default MarkdocRenderer;
