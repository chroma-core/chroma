"use client";

import { motion } from "framer-motion";
import { useRouter } from "next/navigation";

import { PartialAssistantPost, TweetModelBase } from "@/types";

import { useEffect, useState } from "react";
import TweetBody from "./tweet-body";

interface TweetProps {
  tweet: TweetModelBase;
  aiReply?: PartialAssistantPost | TweetModelBase;
  className?: string;
}

export function Tweet({ tweet, aiReply, className = "" }: TweetProps) {
  const router = useRouter();
  const [reply, setReply] = useState<PartialAssistantPost | TweetModelBase | undefined>(undefined);

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
  }, [tweet.aiReplyId]);

  const formattedDate = new Date(tweet.date * 1000).toLocaleDateString('en-US', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });

  const goToPostPage = () => {
    router.push(`/post/${tweet.id}`);
  };

  let aiReplyComponent = null;
  const aiReplyClassName = "text-sm text-gray-500 font-ui mt-2 min-h-[1em]";
  const aiReplyProps = { className: aiReplyClassName, citationsCollapsedByDefault: true };
  if (reply && 'stream' in reply) {
    aiReplyComponent = <TweetBody stream={reply.stream} {...aiReplyProps} /> ;
  } else if (reply) {
    aiReplyComponent = <TweetBody body={reply.body} citations={reply.citations} {...aiReplyProps} />;
  }

  return (
    <div
      className={`grid grid-cols-[120px_1fr] hover:bg-gray-100 cursor-pointer ${className}`}
      onClick={goToPostPage}
    >
      <div className="flex flex-col items-end">
        <div className={`font-ui pl-2 pr-4 pt-4 mt-[.0em] pb-4 text-gray-600 text-sm`}>{formattedDate}</div>
      </div>
      <div className={`pt-4 pb-4 pl-4 pr-4 border-l-[.5px]`}>
        <TweetBody body={tweet.body} citations={tweet.citations} className={className} />
        <motion.div
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          transition={{ duration: 0.2, ease: "easeOut" }}
        >
        {reply && aiReplyComponent}
        </motion.div>
      </div>
    </div>
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
