"use client";

import { useEffect, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { Tweet, TweetSkeleton } from "@/components/tweet";
import TweetPrompt from "@/components/tweet-prompt";
import { PostModel, Role } from "@/types";
import { getPosts, publishNewPost } from "@/actions";
import { groupPostsByMonthAndYear } from "@/util";

export default function Home() {
  const [madePost, setMadePost] = useState<boolean>(false);
  const [loadingMessages, setLoadingMessages] = useState<boolean>(true);
  const [oldMessages, setOldMessages] = useState<
    { month: string; posts: PostModel[] }[]
  >([]);
  const [newMessages, setNewMessages] = useState<Array<PostModel>>([]);

  useEffect(() => {
    getPosts().then((posts) => {
      const postsGroupedByMonth = groupPostsByMonthAndYear(posts).reverse();
      setOldMessages((_) => postsGroupedByMonth);
      setLoadingMessages(false);
    });
  }, []);

  const tweets = loadingMessages ? (
    Array.from(new Array(4), (x, i) => <TweetSkeleton key={i} />)
  ) : (
    <>
      {newMessages.map((m, i) => (
        <motion.li
          key={m.id}
          initial={
            madePost
              ? { opacity: 0, height: 0 }
              : { opacity: 1, height: "auto" }
          }
          animate={{ opacity: 1, height: "auto" }}
        >
          <Tweet tweet={m} animate={true} />
        </motion.li>
      ))}
      {oldMessages.map(({ month, posts }, i) => (
        <motion.li key={month} className="flex flex-col gap-4">
          <h2 className="text-xl mt-4">{month}</h2>
          {posts.map((p, i) => (
            <Tweet key={p.id} tweet={p} animate={false} />
          ))}
        </motion.li>
      ))}
    </>
  );

  return (
    <div className="flex flex-row justify-center py-20 bg-white  mb-48">
      <div className="flex flex-col justify-between items-stretch gap-4 w-[500px] max-w-[calc(100dvw-32px)]">
        <TweetPrompt
          onSubmit={(input) => {
            setMadePost(true);
            publishNewPost(input).then((newTweet) => {
              setNewMessages((tweets) => [newTweet, ...tweets]);
            });
          }}
        />
        <ul className="flex flex-col items-stretch gap-6 h-full items-center">
          <AnimatePresence initial={false}>{tweets}</AnimatePresence>
        </ul>
      </div>
    </div>
  );
}
