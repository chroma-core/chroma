"use client";

import { useEffect, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { Tweet, TweetSkeleton } from "@/components/tweet";
import TweetPrompt from "@/components/tweet-prompt";
import { TweetModel, Role } from "@/types";
import { getPosts, publishNewUserPost } from "@/actions";
import { AiOutlineRobot } from "react-icons/ai";

export default function Home() {
  const [newMessages, setNewMessages] = useState<Array<TweetModel>>([]);

  return (
    <div className="flex flex-row justify-center py-20 mb-48">
      <div className="flex flex-col justify-between items-stretch w-[500px] max-w-[calc(100dvw-32px)]">
        <div className="border-b-[.5px] border-gray-400 pb-4">
          <TweetPrompt
            onSubmit={(input) => {
              publishNewUserPost(input).then((newTweet) => {
                setNewMessages((tweets) => [newTweet, ...tweets]);
              });
            }}
          />
          <IntroTweet />
        </div>
        <ul className="flex flex-col items-stretch h-full items-center">
          <Tweets newMessages={newMessages} />
        </ul>
      </div>
    </div>
  );
}

function IntroTweet() {
  return (
    <div className="w-full flex flex-row gap-4 mt-4 mx-2">
      <div className="pt-[.2em]">
        <AiOutlineRobot size={20} />
      </div>
      <div className="flex flex-col w-full items-stretch gap-2">
        <div className="text-sm">
          Hey! I'm your personal assistant. If you ever need help to remember anything, just mention me using <span className="font-bold text-[#545e51]">@assistant</span>
        </div>
      </div>
    </div>
  );
}

function Tweets({ newMessages }: { newMessages: TweetModel[] }) {
  const [loadingMessages, setLoadingMessages] = useState<boolean>(true);
  const [oldMessages, setOldMessages] = useState<TweetModel[]>([]);

  useEffect(() => {
    getPosts().then((posts) => {
      setOldMessages((_) => posts.reverse());
      setLoadingMessages(false);
    });
  }, []);

  if (loadingMessages) {
    return <TweetSkeleton />;
  }

  if (!loadingMessages && oldMessages.length === 0 && newMessages.length === 0) {
    return <div className="flex flex-row justify-center py-20 mb-48">
      <div>
        <p>No posts yet... Make your first post!</p>
      </div>
    </div>;
  }

  return <AnimatePresence>
    {newMessages.map((m, i) => (
      <motion.li
        key={m.id}
        initial={{ opacity: 0, height: 0 }}
        animate={{ opacity: 1, height: "auto" }}
      >
        <Tweet tweet={m} animate={true} />
      </motion.li>
    ))}

    <li className="flex flex-col">
      {oldMessages.map((p) => (
        <Tweet key={p.id} tweet={p} animate={false} />
      ))}
    </li>
  </AnimatePresence>
}
