import fs from "fs";
import path from "path";
import React from "react";
import Markdoc from "@markdoc/markdoc";
import markdocConfig from "@/markdoc/config";

const MarkdocRenderer: React.FC<{ slug: string[] }> = ({ slug }) => {
  const filePath = `${path.join(process.cwd(), "markdoc", "content", ...slug)}.md`;

  if (!fs.existsSync(filePath)) {
    return <h1>404 - Page Not Found</h1>;
  }

  const source = fs.readFileSync(filePath, "utf-8");

  const ast = Markdoc.parse(source);
  const content = Markdoc.transform(ast, markdocConfig);

  const output = Markdoc.renderers.react(content, React);

  return (
    <div className="w-full max-w-full h-full overflow-y-scroll py-10 px-14 pl-20 prose dark:prose-invert">
      {output}
    </div>
  );
};

export default MarkdocRenderer;
