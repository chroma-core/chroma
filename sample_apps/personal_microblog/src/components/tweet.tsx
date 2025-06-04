"use client";

import { motion } from "framer-motion";
import { useRouter } from "next/navigation";

import { PartialAssistantPost, TweetModel } from "@/types";

import { MarkdownContent, StreamedMarkdownContent } from "./markdown-content";
import { useEffect, useState } from "react";
import { formatDate } from "@/util";
import { readStreamableValue, StreamableValue } from "ai/rsc";

interface TweetProps {
  tweet: TweetModel;
  aiReply?: PartialAssistantPost;
  className?: string;
}

export function Tweet({ tweet, aiReply, className }: TweetProps) {
  const router = useRouter();
  const [reply, setReply] = useState<PartialAssistantPost | undefined>(undefined);

  useEffect(() => {
    if (aiReply) {
      setReply(aiReply);
    } else if (tweet.aiReplyId) {
      fetch(`/api/post/${tweet.aiReplyId}`).then(async (res) => {
        const json = await res.json();
        if (json) {
          setReply(json);
        }
      });
    }
  }, []);

  const formattedDate = new Date(tweet.date * 1000).toLocaleDateString('en-US', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });

  let replyCitations = undefined;
  if (reply) {
    replyCitations = <StreamedCitations citations={reply.citations || reply.citationStream} />;
  }

  let replyContent = undefined;
  if (reply?.bodyStream) {
    replyContent = <StreamedMarkdownContent stream={reply.bodyStream} placeholder="Thinking..." className={`${className} text-[.9em]/[1.3] font-ui text-gray-500`} />;
  } else if (reply?.body) {
    replyContent = <MarkdownContent content={reply.body} className={`${className} text-[.9em]/[1.3] font-ui text-gray-500`} />
  }

  const handleClick = () => {
    router.push(`/post/${tweet.id}`);
  };

  return (
    <motion.div
      className={`grid grid-cols-[120px_1fr] hover:bg-gray-100 cursor-pointer ${className}`}
      onClick={handleClick}
    >
      <div className="flex flex-col items-end">
        <div className={`font-ui pl-2 pr-4 pt-4 mt-[.0em] pb-4 text-gray-600 text-sm`}>{formattedDate}</div>
      </div>
      <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
        <MarkdownContent content={tweet.body} className={`${className} text-[.95em]/[1.3] font-body`} />
        {tweet.citations.length > 0 && (
          <div className="mt-2 flex flex-col gap-2">
            {tweet.citations.map((citation) => (
              <Citation key={citation} citationId={citation} />
            ))}
          </div>
        )}
        <div className="mt-2">
          {replyContent}
          {replyCitations}
        </div>
      </div>
    </motion.div>
  );
}

function Citation({ citationId }: { citationId: string }) {
  if (citationId == undefined || citationId.length == 0) {
    return null;
  }

  const [citation, setCitation] = useState<TweetModel | undefined>(undefined);

  useEffect(() => {
    fetch(`/api/post/${citationId}`).then(async (res) => {
      const json = await res.json();
      setCitation(json);
    });
  }, []);

  const snippet = citation?.body?.split('\n')[0]?.slice(0, 100);
  const hasMore = snippet?.length && snippet.length < (citation?.body?.length ?? 0);

  return (
    citation &&
    <motion.div
      className="bg-gray-100 rounded-md p-2"
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
    >
      <a href={`/post/${citationId}`} key={citationId}>({formatDate(citation.date)}) {snippet}{hasMore && '...'}</a>
    </motion.div>
  );
}

function StreamedCitations({ citations: citationStream }: { citations: StreamableValue<string, any> | string[] }) {
  const [citations, setCitations] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (Array.isArray(citationStream)) {
      setCitations(citationStream);
      setIsLoading(false);
      return;
    }

    const streamContent = async () => {
      if (!citationStream) {
        return;
      }
      try {
        for await (const citation of readStreamableValue(citationStream)) {
          if (citation) {
            setCitations((prev) => [...prev, citation]);
          }
        }
      } catch (error) {
        console.error('Streaming error:', error);
      }
      setIsLoading(false);
    };

    streamContent();
  }, [citationStream]);

  return (
    (!isLoading && citations.length > 0) && (
      <div className="mt-2 flex flex-col gap-2 text-sm text-gray-500">
        {citations.map((citation) => (
          <Citation key={citation} citationId={citation} />
        ))}
      </div>
    )
  );
}

export function TweetSkeleton() {
  return (
    <div className={`grid grid-cols-[120px_1fr] min-h-[500px]`}>
      <div className="">
      </div>
      <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
        <div>Loading...</div>
      </div>
    </div>
  );
}
