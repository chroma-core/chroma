import { StreamableValue } from "ai/rsc";
import { MarkdownContent, StreamedMarkdownContent } from "./markdown-content";
import Citations from "./citations";

interface TweetBodyProps {
  body: string | StreamableValue<string, any>;
  citations?: string[] | StreamableValue<string, any>;
  className?: string;
  bodyClassName?: string;
  citationsClassName?: string;
  citationsCollapsedByDefault?: boolean;
}

export default function TweetBody({ body, citations, className = "", bodyClassName = "", citationsClassName = "", citationsCollapsedByDefault = false }: TweetBodyProps) {
  const bodyComponent = typeof body === 'string' ? <MarkdownContent content={body} className={bodyClassName} /> : <StreamedMarkdownContent stream={body} className={bodyClassName} />;
  const citationsComponent = Array.isArray(citations) ? <Citations citationIds={citations} collapsedByDefault={citationsCollapsedByDefault} /> : <Citations citationStream={citations} collapsedByDefault={citationsCollapsedByDefault} />;
  return (
    <div className={`${className}`}>
      {bodyComponent}
      <div className="mt-2">
        {citationsComponent}
      </div>
    </div>
  );
}
