"use client";

import { motion } from "framer-motion";

import { useEffect, useState } from "react";
import { TweetModel } from "@/types";
import { getPostById } from "@/actions";

import MarkdownContent from "./markdown-content";

interface TweetProps {
  tweet: TweetModel;
  className?: string;
}

export function Tweet({ tweet, className }: TweetProps) {
  const [reply, setReply] = useState<TweetModel | null>(null);
  const hasReply = tweet.aiReplyId !== undefined && tweet.aiReplyId !== "";

  useEffect(() => {
    if (hasReply && tweet.aiReplyId) {
      getPostById(tweet.aiReplyId).then(setReply);
    }
  }, [tweet.aiReplyId, hasReply]);

  const formattedDate = new Date(tweet.date * 1000).toLocaleDateString('en-US', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });

  const padding = tweet.body.length > 40 || tweet.body.split("\n").length > 2 ? "4" : "4";

  let replyContent = null;
  if (reply && reply.status == 'done') {
    replyContent = <MarkdownContent content={reply.body} className={`${className} font-ui`} />
  }
  if (reply && reply.status === 'error') {
    replyContent = <p>Sorry, an error occurred while trying to answer your question.</p>
  } else if ((reply && (reply.status === 'created' || reply.status === 'processing'))) {
    replyContent = <p>Generating reply...</p>
  }

  return (
    <a href={`/post/${tweet.id}`}>
      <motion.div className={`grid grid-cols-[120px_1fr] hover:bg-gray-100 ${className}`}>
        <div className="flex flex-col items-end">
          <div className={`font-ui pl-2 pr-4 pt-${padding} mt-[.0em] pb-4 text-gray-600 text-sm`}>{formattedDate}</div>
        </div>
        <div className={`pt-${padding} pb-${padding} pl-4 pr-4 border-l-[.5px]`}>
          <MarkdownContent content={tweet.body} className={`${className} text-[.95em]/[1.3] font-light font-body`} />
          {reply && <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            transition={{ duration: 0.2 }}
          >
            <div className="font-ui text-gray-500 text-[.75em]/[1.3] mt-2">
              {replyContent}
            </div>
          </motion.div>}
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
