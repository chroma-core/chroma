"use client";

import { useEffect, useState, useRef } from "react";
import { AnimatePresence, motion } from "framer-motion";
import { Tweet, TweetSkeleton } from "@/components/tweet";
import TweetPrompt from "@/components/tweet-prompt";
import { TweetModel } from "@/types";
import { getPosts, publishNewUserPost } from "@/actions";
import Logo from "./logo";

export default function FeedView() {
  const [newMessages, setNewMessages] = useState<Array<TweetModel>>([]);

  return (
    <>
      <div className="border-b pb-4">
        <div className="pb-4">
          <IntroTweet />
        </div>
        <TweetPrompt
          onSubmit={(input) => {
            publishNewUserPost(input).then((newTweet) => {
              setNewMessages((tweets) => [newTweet, ...tweets]);
            });
          }}
        />
      </div>
      <ul className="flex flex-col items-stretch h-full items-center">
        <Tweets newMessages={newMessages} />
      </ul>
    </>
  );
}

function IntroTweet() {
  return (
    <div className="w-full flex flex-row gap-2 mt-4 mx-2">
      <div className="pt-[.2em] text-gray-700">
        <Logo size={24} />
      </div>
      <div className="flex flex-col w-full items-stretch gap-2">
        <div className="font-ui text-sm">
          Hey! I'm your personal assistant. If you ever need help to remember anything, just mention me using <span className="font-bold text-[var(--accent)]">@assistant</span>
        </div>
      </div>
    </div>
  );
}

function Tweets({ newMessages }: { newMessages: TweetModel[] }) {
  const [loadingMessages, setLoadingMessages] = useState<boolean>(true);
  const [oldMessages, setOldMessages] = useState<TweetModel[]>([]);
  const [currPage, setCurrPage] = useState<number>(0);
  const [isLoadingMore, setIsLoadingMore] = useState<boolean>(false);
  const [hasMore, setHasMore] = useState<boolean>(true);

  // Initial load
  useEffect(() => {
    const loadInitialPosts = async () => {
      try {
        const posts = await getPosts(0);
        setOldMessages(posts);
        setHasMore(posts.length > 0);
        setLoadingMessages(false);
      } catch (error) {
        console.error('Error loading initial posts:', error);
        setLoadingMessages(false);
      }
    };

    loadInitialPosts();
  }, []);

  useEffect(() => {
    if (currPage === 0) return;

    const loadMorePosts = async () => {
      if (isLoadingMore || !hasMore) return;

      setIsLoadingMore(true);
      try {
        const posts = await getPosts(currPage);
        if (posts.length === 0) {
          setHasMore(false);
        } else {
          setOldMessages(prev => [...prev, ...posts]);
        }
      } catch (error) {
        console.error('Error loading more posts:', error);
      } finally {
        setIsLoadingMore(false);
      }
    };

    loadMorePosts();
  }, [currPage]);

  // Window scroll event listener for infinite scroll
  useEffect(() => {
    const onScroll = () => {
      if (hasMore && !isLoadingMore) {
        const { scrollTop, scrollHeight, clientHeight } = document.documentElement;
        if (scrollTop + clientHeight >= scrollHeight - 100) {
          setCurrPage(prev => prev + 1);
        }
      }
    };

    window.addEventListener('scroll', onScroll);
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  if (loadingMessages) {
    return <TweetSkeleton />;
  }

  if (!loadingMessages && oldMessages.length === 0 && newMessages.length === 0) {
    return <div className="flex flex-row font-ui justify-center py-20 mb-48">
      <div>
        <p>No posts yet... Make your first post!</p>
      </div>
    </div>;
  }

  return (
    <div>
      <AnimatePresence>
        {newMessages.map((m, i) => (
          <motion.li
            key={m.id}
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
          >
            <Tweet tweet={m} />
          </motion.li>
        ))}

        <li className="flex flex-col">
          {oldMessages.map((p) => (
            <Tweet key={p.id} tweet={p} />
          ))}
        </li>
      </AnimatePresence>

      {!hasMore && oldMessages.length > 0 && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="font-ui flex justify-center py-8 text-gray-500">
          <p>You've reached the end!</p>
        </motion.div>
      )}
    </div>
  );
}
