"use client";

import { ReactNode, useEffect, useRef, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { Tweet, TweetSkeleton } from "@/components/tweet";
import TweetPrompt from "@/components/tweet-prompt";
import { PostModel, Role } from "@/types";
import { getPosts } from "@/actions/posts";

function makeTweetEntry(role: Role, body: string): PostModel {
  return {
    id: crypto.randomUUID(),
    role: role,
    body: body,
    date: new Date().toISOString(),
  };
}

const introTweet = makeTweetEntry(
  "assistant",
  "Hey! I'm your personal assistant! If you ever need my help remembering something, just mention me with @assistant"
);

export default function Home() {
  const [loadingMessages, setLoadingMessages] = useState<boolean>(true);
  const [messages, setMessages] = useState<Array<PostModel>>([]);

  useEffect(() => {
    getPosts().then((posts) => {
      setMessages((tweets) => [introTweet, ...posts]);
      setLoadingMessages(false);
    });
  }, []);

  const tweets = loadingMessages ? (
    Array.from(new Array(4), (x, i) => <TweetSkeleton key={i} />)
  ) : (
    <>
      {messages.map((m, i) => (
        <motion.li
          key={m.id}
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: "auto" }}
        >
          <Tweet tweet={m} />
        </motion.li>
      ))}
    </>
  );

  return (
    <div className="flex flex-row justify-center py-20 h-dvh bg-white dark:bg-zinc-900">
      <div className="flex flex-col justify-between items-stretch gap-4 w-[500px] max-w-[calc(100dvw-32px)]">
        <TweetPrompt
          onSubmit={(input) => {
            const newTweet = makeTweetEntry("user", input);
            setMessages((tweets) => [newTweet, ...tweets]);
          }}
        />
        <ul className="flex flex-col items-stretch gap-3 h-full items-center overflow-y-scroll">
          <AnimatePresence initial={false}>{tweets}</AnimatePresence>
        </ul>
      </div>
    </div>
  );
}
