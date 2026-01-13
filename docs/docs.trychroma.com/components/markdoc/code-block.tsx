import React from "react";
import { unified } from "unified";
import parse from "remark-parse";
import rehypeHighlight from "rehype-highlight";
import remarkRehype from "remark-rehype";
import rehypeStringify from "rehype-stringify";
import { visit } from "unist-util-visit";
import CodeBlockHeader from "@/components/markdoc/code-block-header";

export interface CodeBlockProps {
  content: React.ReactNode;
  language: string;
  showHeader: boolean;
  className?: string;
}

const rehypeRemovePre = () => {
  return (tree: any) => {
    visit(tree, "element", (node) => {
      if (node.tagName === "pre" && node.children.length) {
        const codeNode = node.children.find(
          (child: { tagName: string }) => child.tagName === "code",
        );
        if (codeNode) {
          node.tagName = "code";
          node.children = codeNode.children;
        }
      }
    });
  };
};

const CodeBlock: React.FC<CodeBlockProps> = async ({
  content,
  language,
  showHeader = true,
  className,
}) => {
  if (typeof content !== "string") {
    throw new Error("CodeBlock children must be a string.");
  }

  const highlightedCode = await unified()
    .use(parse)
    .use(remarkRehype)
    .use(rehypeHighlight, { subset: [language] })
    .use(rehypeRemovePre)
    .use(rehypeStringify)
    .process(`\`\`\`${language}\n${content}\`\`\``);

  return (
    <div className="flex flex-col mb-2">
      {showHeader && language && (
        <CodeBlockHeader language={language} content={content} />
      )}
      <pre
        className={`rounded-none rounded-b-sm m-0 bg-[var(--tw-prose-invert-bg)] dark:bg-black text-black dark:text-[var(--tw-prose-pre-code)] border border-t-0 dark:border-gray-700 ${className ? className : ""}`}
      >
        <div
          dangerouslySetInnerHTML={{
            __html: highlightedCode.toString().replace("```", ""),
          }}
        />
      </pre>
    </div>
  );
};
CodeBlock.displayName = "CodeBlock";

export default CodeBlock;
