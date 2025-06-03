import React, { useState, useEffect } from "react";
import * as production from 'react/jsx-runtime';
import { remark } from "remark";
import remarkRehype from "remark-rehype";
import rehypeReact from "rehype-react";
import styles from "./markdown-content.module.css";
import { AnchorTag, Strong } from "@/markdown";
import { readStreamableValue, StreamableValue } from "ai/rsc";

const markdownPipeline = remark()
  .use(remarkPluginReplaceMatch(/@assistant/, (match) => {
    return {
      type: "strong",
      children: [{ type: "text", value: match }],
    }
  }))
  .use(remarkPluginReplaceMatch(/\$[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/i, (match) => {
    return {
      type: "link",
      url: `${match}`,
      children: [{ type: "text", value: match }],
    }
  }))
  .use(remarkRehype)
  .use(rehypeReact, {
    ...production, components: {
      a: AnchorTag,
      strong: Strong,
    }
  })

export function MarkdownContent({ content, className }: { content: string, className?: string }) {
  const [rendering, setRendering] = useState(true);
  const [htmlBody, setHtmlBody] = useState(<></>);
  const [estimatedLines, setEstimatedLines] = useState(3);

  useEffect(() => {
    markdownPipeline
      .process(content)
      .then((result: { result: any }) => {
        setRendering(false);
        setHtmlBody(result.result);
      })
      .catch((err: any) => {
        setHtmlBody(
          <>
            <span className="error">{err}</span>
            <pre>{content}</pre>
          </>
        );
      });
    setEstimatedLines(content.length / 30);
  }, [content]);

  if (rendering) {
    return <MarkdownContentSkeleton lines={estimatedLines} className={className} />;
  }
  return <div className={`w-full ${styles.markdown} ${className}`}>
    {htmlBody}
  </div>;
}

export function StreamedMarkdownContent({ stream, placeholder, className }: { stream: StreamableValue<string, any>, placeholder?: string, className?: string }) {
  const [rendering, setRendering] = useState(true);
  const [content, setContent] = useState<string>(placeholder ?? '');
  const [htmlBody, setHtmlBody] = useState(<></>);
  const [estimatedLines, setEstimatedLines] = useState(3);

  useEffect(() => {
    if (!stream) {
      return;
    }

    setHtmlBody(<></>);

    const streamContent = async () => {
      if (!stream) {
        return;
      }
      try {
        for await (const content of readStreamableValue(stream)) {
          if (content) {
            setContent(content);
          }
        }
      } catch (error) {
        console.error('Streaming error:', error);
      }
    };

    streamContent();
  }, [stream]);

  useEffect(() => {
    markdownPipeline
      .process(content)
      .then((result: { result: any }) => {
        setRendering(false);
        setHtmlBody(result.result);
      })
      .catch((err: any) => {
        setHtmlBody(
          <>
            <span className="error">{err}</span>
            <pre>{content}</pre>
          </>
        );
      });
    setEstimatedLines(content.length / 30);
  }, [content]);

  if (rendering) {
    return <MarkdownContentSkeleton lines={estimatedLines} className={className} />;
  }
  return <div className={`w-full ${styles.markdown} ${className}`}>
    {htmlBody}
  </div>;
}

function MarkdownContentSkeleton({ lines, className }: { lines: number, className?: string }) {
  return <div className={`flex flex-col gap-1 w-full ${styles.markdown} ${className}`}>
    {Array.from({ length: lines }).map((_, i) => (
      <div key={i} className={`h-4 bg-gray-100 rounded-full animate-pulse mr-1`} />
    ))}
  </div>;
}

function remarkPluginReplaceMatch(find: RegExp, replace: (match: string) => any) {
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
          return replaceMatches(node.value, find, replace);
        } else {
          return node;
        }
      });
    } else if (tree.children) {
      tree.children = tree.children.map((node: any) => visit(node));
    }
    return tree;
  }
  return () => visit;
}

function replaceMatches(body: string, find: RegExp, replace: (match: string) => any): any[] {
  const parts: any[] = [];
  let lastIndex = 0;

  if (!body || body.length === 0) {
    return [];
  }

  if (!find.test(body)) {
    return [{ type: "text", value: body }];
  }

  body.replace(find, (match, offset) => {
    // Push text before
    if (lastIndex < offset) {
      const content = body.slice(lastIndex, offset);
      parts.push({
        type: "text",
        value: content,
      });
    }
    //Push new node
    parts.push(replace(match));
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
