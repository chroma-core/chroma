"use client";

import { motion } from "framer-motion";
import { AiOutlineUser } from "react-icons/ai";

import { useEffect, useState } from "react";
import { TweetModel, Role } from "@/types";
import { getPostById } from "@/actions";

import MarkdownContent from "./markdown-content";

const iconSize = 20;

interface TweetProps {
  tweet: TweetModel;
  animate?: boolean;
  className?: string;
}

export function Tweet({ tweet, animate, className }: TweetProps) {
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

  return (
    <a href={`/post/${tweet.id}`}>
      <motion.div className={`grid grid-cols-[120px_1fr] hover:bg-gray-100 ${className}`}>
        <div className="flex flex-col items-end">
          <div className={`pl-2 pr-4 pt-${padding} pb-4 text-sm text-gray-800`}>{formattedDate}</div>
        </div>
        <div className={`pt-${padding} pb-${padding} pl-4 pr-4 border-l`}>
          <MarkdownContent content={tweet.body} className={className} />
        </div>
      </motion.div>
    </a>
  );
}

export function TweetSkeleton() {
  return (
    <div className="w-full flex flex-row gap-4">
      <div className="pt-[.2em]">
        <AiOutlineUser size={iconSize} />
      </div>
      <div className="flex flex-col w-full items-stretch gap-2">
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-1`} />
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-2`} />
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-4`} />
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-1`} />
      </div>
    </div>
  );
}
