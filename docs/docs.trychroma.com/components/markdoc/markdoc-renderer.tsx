import fs from "fs";
import path from "path";
import React from "react";
import Markdoc from "@markdoc/markdoc";
import markdocConfig from "@/markdoc/config";
import { notFound } from "next/navigation";
import MarkdocPage from "@/components/markdoc/markdoc-page";
import SidebarToggle from "@/components/header/sidebar-toggle";

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

  return (
    <MarkdocPage>
      <div className="relative w-full h-full">
        <SidebarToggle path={slug} />
        {output}
      </div>
    </MarkdocPage>
  );
};

export default MarkdocRenderer;
