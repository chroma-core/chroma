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
  .use(remarkPluginReplaceMatch(/\[($|\^($|(\d+)($|\])))/, (match) => {
    // This matches citation references like [^1], [^2], etc.
    // This also matches all prefixes of it like [, [^, or [^ in the case
    // that when the AI's response is being streamed, it only gives us part
    // of the citation reference.
    // There is an edge case where the backend is streaming text and it only
    // sends part of the citation reference. If this happens, the user will see
    // something like [^ for a split second before it can properly be parsed
    // and rendered.
    // We want to hide partial potential citation references, and only show
    // the citation reference when we know for sure it's a citation reference,
    // or hide it until more information is streamed.

    const citationMatch = match.match(/\[\^(\d+)\]/);
    if (citationMatch) {
      return {
        type: "link",
        url: `#citation-${citationMatch[1]}`,
        children: [{ type: "text", value: citationMatch[0] }],
      }
    } else {
      return {
        type: "text",
        value: "",
      }
    }
  }))
  .use(remarkRehype)
  .use(rehypeReact, {
    ...production, components: {
      a: AnchorTag,
      strong: Strong,
    }
  })

export function MarkdownContent({ content, placeholder, className }: { content: string, placeholder?: string, className?: string }) {
  const [rendering, setRendering] = useState(true);
  const [htmlBody, setHtmlBody] = useState(<></>);
  const [estimatedLines, setEstimatedLines] = useState(3);

  useEffect(() => {
    markdownPipeline
      .process(content ?? placeholder ?? "")
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
    const lines = content.split("\n");
    const estimatedLineCount = Math.max(1, Math.floor(lines.reduce((acc, line) => acc + (line.length / 30), 0)));
    setEstimatedLines(estimatedLineCount);
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
      <div key={i} className={`h-4 bg-[var(--background-secondary)] rounded-full animate-pulse mr-1`} />
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

  const globalFind = find.global ? find : new RegExp(find.source, find.flags + 'g');

  if (!globalFind.test(body)) {
    return [{ type: "text", value: body }];
  }

  // Reset lastIndex since we used test() above
  globalFind.lastIndex = 0;

  body.replace(globalFind, (match, ...args) => {
    const offset = args[args.length - 2];
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
