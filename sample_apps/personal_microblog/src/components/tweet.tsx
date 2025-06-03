"use client";

import { motion } from "framer-motion";

import { TweetModel } from "@/types";

import MarkdownContent from "./markdown-content";
import { readStreamableValue, StreamableValue } from "ai/rsc";
import { useEffect, useState } from "react";

interface TweetProps {
  tweet: TweetModel;
  bodyStream?: StreamableValue<string, any>;
  className?: string;
}

export function Tweet({ tweet, bodyStream, className }: TweetProps) {
  const [body, setBody] = useState<string>(tweet.body ?? '');
  const [isStreaming, setIsStreaming] = useState<boolean>(false);

  useEffect(() => {
    if (!bodyStream) {
      return;
    }

    setIsStreaming(true);
    setBody('');

    const streamContent = async () => {
      try {
        for await (const content of readStreamableValue(bodyStream)) {
          if (content) {
            setBody(content);
          }
        }
      } catch (error) {
        console.error('Streaming error:', error);
      } finally {
        setIsStreaming(false);
      }
    };

    streamContent();
  }, [bodyStream]);

  const formattedDate = new Date(tweet.date * 1000).toLocaleDateString('en-US', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });

  return (
    <a href={`/post/${tweet.id}`}>
      <motion.div className={`grid grid-cols-[120px_1fr] hover:bg-gray-100 ${className}`}>
        <div className="flex flex-col items-end">
          <div className={`font-ui pl-2 pr-4 pt-4 mt-[.0em] pb-4 text-gray-600 text-sm`}>{formattedDate}</div>
        </div>
        <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
          <MarkdownContent content={body} className={`${className} text-[.95em]/[1.3] font-body ${tweet.role === "assistant" ? "font-bold text-blue-600" : ""}`} />
        </div>
      </motion.div>
    </a>
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
