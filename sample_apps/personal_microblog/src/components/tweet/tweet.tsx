"use client";

import { motion } from "framer-motion";
import { useRouter } from "next/navigation";

import { PartialAssistantPost, TweetModel, TweetModelBase } from "@/types";

import { useEffect, useState } from "react";
import TweetBody from "./tweet-body";
import { formatDate } from "@/util";

interface TweetProps {
  tweet: TweetModel;
  className?: string;
}

export function Tweet({ tweet, className = "" }: TweetProps) {
  const [reply, setReply] = useState<PartialAssistantPost | TweetModelBase | undefined>(undefined);

  const formattedDate = formatDate(tweet.date);

  const router = useRouter();
  const goToPostPage = () => {
    router.push(`/post/${tweet.id}`);
  };

  const isAiPost = tweet.role === "assistant";


  let mainBodyProps: object = {
    className: 'font-body mt-2 min-h-[1em]',
  };
  let mainCitationProps: object = {
    animate: false,
    style: "block"
  };
  if (isAiPost) {
    mainBodyProps = {
      className: 'font-ui mt-2 min-h-[1em] opacity-80 text-[var(--accent)]',
    };
    mainCitationProps = {
      animate: true,
      collapsedByDefault: true,
      style: "endnote",
      className: 'font-ui text-[var(--accent)] text-xs'
    };
  }

  useEffect(() => {
    switch (tweet.type) {
      case 'streaming':
        setReply(tweet.aiReply);
        break;
      case 'enriched':
        setReply(tweet.enrichedAiReply);
        break;
      case 'base':
        if (tweet.aiReplyId) {
          fetch(`/api/post/${tweet.aiReplyId}`).then(async (res) => {
            const json: TweetModelBase = await res.json();
            if (json) {
              setReply(json);
            }
          });
        }
        break;
    }
  }, [tweet.type, tweet.aiReplyId]);

  let aiReplyComponent = null;
  const aiReplyProps = {
    className: 'text-[.85em]/5 font-ui mt-2 min-h-[1em] opacity-80 text-[var(--accent)]',
    citationsProps: {
      collapsedByDefault: true,
      animate: true,
      style: "endnote",
    } };
  const streamedAiReply = reply && 'stream' in reply;
  if (streamedAiReply) {
    aiReplyComponent = <TweetBody stream={reply.stream} {...aiReplyProps} /> ;
  } else if (reply) {
    aiReplyComponent = <TweetBody body={reply.body} citations={reply.citations} {...aiReplyProps} />;
  }

  return (
    <div
      className={`grid grid-cols-[140px_1fr] hover:bg-gray-100 cursor-pointer ${className}`}
      onClick={goToPostPage}
    >
      <div className="flex flex-col items-end">
        <div className={`font-ui pl-2 pr-4 pt-4 mt-[.6em] pb-4 text-gray-600 text-sm`}>{formattedDate}</div>
      </div>
      <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
        <TweetBody body={tweet.body} citations={tweet.citations} className={className} bodyProps={mainBodyProps} citationsProps={mainCitationProps} />
        <motion.div
          initial={streamedAiReply ? { opacity: 0, height: 0 } : { }}
          animate={streamedAiReply ? { opacity: 1, height: 'auto' } : { }}
          transition={streamedAiReply ? { duration: 0.2, ease: "easeOut" } : {}}
        >
        {reply && aiReplyComponent}
        </motion.div>
      </div>
    </div>
  );
}

export function TweetSkeleton() {
  return (
    <div className={`grid grid-cols-[120px_1fr] animate-pulse`}>
      <div className="flex flex-col items-end">
        <div className="font-ui pl-2 pr-4 pt-4 mt-[.0em] pb-4">
          <div className="h-4 w-16 bg-gray-200 rounded"></div>
        </div>
      </div>
      <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
        <div className="space-y-3">
          <div className="h-4 bg-gray-200 rounded w-full"></div>
          <div className="h-4 bg-gray-200 rounded w-5/6"></div>
          <div className="h-4 bg-gray-200 rounded w-4/6"></div>
        </div>
        <div className="mt-4 space-y-2">
          <div className="h-3 bg-gray-100 rounded w-3/4"></div>
          <div className="h-3 bg-gray-100 rounded w-1/2"></div>
        </div>
      </div>
    </div>
  );
}
