import React, { useState, useEffect } from "react";
import { remark } from "remark";
import remarkHtml from "remark-html";
import styles from "./markdown-content.module.css";

export default function MarkdownContent({ content, className }: { content: string, className?: string }) {
  const [rendering, setRendering] = useState(true);
  const [htmlBody, setHtmlBody] = useState(content);
  const [estimatedLines, setEstimatedLines] = useState(3);

  useEffect(() => {
    remark()
      .use(remarkHtml)
      .use(remarkCustom)
      .process(content)
      .then((result) => {
        setRendering(false);
        setHtmlBody(result.toString());
      });
    setEstimatedLines(content.length / 30);
  }, [content]);

  if (rendering) {
    return <MarkdownContentSkeleton lines={estimatedLines} className={className} />;
  }

  return <div
    className={`w-full ${styles.markdown} ${className}`}
    dangerouslySetInnerHTML={{ __html: htmlBody }}
  ></div>;
}

function MarkdownContentSkeleton({ lines, className }: { lines: number, className?: string }) {
  return <div className={`flex flex-col gap-1 w-full ${styles.markdown} ${className}`}>
    {Array.from({ length: lines }).map((_, i) => (
      <div key={i} className={`h-4 bg-gray-100 rounded-full animate-pulse mr-1`} />
    ))}
  </div>;
}

function remarkCustom() {
  /**
   * @param {Root} tree
   * @return {undefined}
   */
  function visit(tree: { children: any; type: string; value: string }) {
    if (tree.type == "code") {
      tree.value = tree.value.trim();
      return tree;
    }
    if (tree.type == "paragraph" && tree.children.length > 0) {
      tree.children = tree.children.flatMap((node: any) => {
        if (node.type == "text") {
          return replaceMentions(node.value);
        } else {
          return node;
        }
      });
    } else if (tree.children) {
      tree.children = tree.children.map((node: any) => visit(node));
    }
    return tree;
  }
  return visit;
}

function replaceMentions(body: string): any[] {
  const parts: any[] = [];
  const mentionRegex = /(@\w+)/g;
  let lastIndex = 0;

  if (!body || body.length === 0) {
    return [];
  }

  if (!mentionRegex.test(body)) {
    return [{ type: "text", value: body }];
  }

  body.replace(mentionRegex, (match, mention, offset) => {
    // Push text before the mention
    if (lastIndex < offset) {
      const content = body.slice(lastIndex, offset);
      parts.push({
        type: "text",
        value: content,
      });
    }
    // Push the mention
    parts.push({
      type: "html",
      children: undefined,
      value: `<span class="text-blue-500">${mention}</span>`,
    });
    lastIndex = offset + match.length;
    return "";
  });

  // Push any remaining text
  if (lastIndex < body.length) {
    const content = body.slice(lastIndex);
    parts.push({
      type: "text",
      value: content,
    });
  }
  return parts;
}
