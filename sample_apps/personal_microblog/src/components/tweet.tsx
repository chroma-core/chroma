"use client";

import { motion } from "framer-motion";

import { PartialAssistantPost, TweetModel } from "@/types";

import { MarkdownContent, StreamedMarkdownContent } from "./markdown-content";
import { useEffect, useState } from "react";
import { getPostById } from "@/actions";

interface TweetProps {
  tweet: TweetModel;
  aiReply?: PartialAssistantPost;
  className?: string;
}

export function Tweet({ tweet, aiReply, className }: TweetProps) {
  const [reply, setReply] = useState<PartialAssistantPost | undefined>(undefined);

  useEffect(() => {
    if (aiReply) {
      setReply(aiReply);
    } else if (tweet.aiReplyId) {
      getPostById(tweet.aiReplyId).then((post: TweetModel | null) => {
        if (post) {
          setReply(post);
        }
      });
    }
  }, []);

  const formattedDate = new Date(tweet.date * 1000).toLocaleDateString('en-US', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });

  let replyContent = undefined;
  if (reply?.stream) {
    replyContent = <StreamedMarkdownContent stream={reply.stream} placeholder="Thinking..." className={`${className} text-[.9em]/[1.3] font-ui text-gray-500`} />;
  } else if (reply?.body) {
    replyContent = <MarkdownContent content={reply.body} className={`${className} text-[.9em]/[1.3] font-ui text-gray-500`} />
  }

  return (
    <a href={`/post/${tweet.id}`}>
      <motion.div className={`grid grid-cols-[120px_1fr] hover:bg-gray-100 ${className}`}>
        <div className="flex flex-col items-end">
          <div className={`font-ui pl-2 pr-4 pt-4 mt-[.0em] pb-4 text-gray-600 text-sm`}>{formattedDate}</div>
        </div>
        <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
          <MarkdownContent content={tweet.body} className={`${className} text-[.95em]/[1.3] font-body`} />
          <div className="mt-2">
            {replyContent}
          </div>
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
