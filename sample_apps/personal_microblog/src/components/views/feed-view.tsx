"use client";

import { useEffect, useState, useRef } from "react";
import { AnimatePresence, m, motion } from "framer-motion";
import { Tweet, TweetSkeleton } from "@/components/tweet/tweet";
import TweetPrompt from "@/components/tweet/tweet-prompt";
import { EnrichedTweetModel, NewPostResponseTweetModel } from "@/types";
import { publishNewUserPost } from "@/actions";
import Logo from "../ui/common/logo";

export default function FeedView() {
  const [newMessages, setNewMessages] = useState<NewPostResponseTweetModel[]>([]);

  return (
    <>
      <div className="border-b pb-4">
        <div className="pb-4">
          <IntroTweet />
        </div>
        <TweetPrompt
          onSubmit={(input) => {
            publishNewUserPost(input).then((tweet) => {
              setNewMessages((tweets) => [tweet, ...tweets]);
            });
          }}
          animate={true}
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
        <a href="https://trychroma.com">
          <Logo size={24} />
        </a>
      </div>
      <div className="flex flex-col w-full items-stretch gap-2">
        <div className="font-ui text-sm">
          Hey! I'm your personal assistant. If you ever need help to remember anything, just mention me using <span className="font-bold text-[var(--accent)]">@assistant</span>
        </div>
      </div>
    </div>
  );
}

function Tweets({ newMessages }: { newMessages: NewPostResponseTweetModel[] }) {
  const [oldMessages, setOldMessages] = useState<EnrichedTweetModel[]>([]);

  // These states are used for infinite scroll pagination
  // we have `page` to keep track of how many "pages" we've loaded and to
  // prevent infinite loops through state change hooks changing their own state
  // `loading` acts like a mutex to prevent multiple requests being made at the same time
  // `cursor` is specific to the pagination implementation
  const [initialLoading, setInitialLoading] = useState<boolean>(true);
  const loadingRef = useRef<boolean>(false);
  const [page, setPage] = useState<number>(0);
  const [cursor, setCursor] = useState<number>(-1);
  const [hasMore, setHasMore] = useState<boolean>(true);

  useEffect(() => {
    if (loadingRef.current || !hasMore) return;

    const loadMorePosts = async () => {
      loadingRef.current = true;
      try {
        let url;
        if (initialLoading) {
          url = "/api/post";
          setInitialLoading(false);
        } else {
          url = `/api/post?cursor=${cursor}`;
        }
        const { posts, cursor: newCursor } = await fetch(url).then(res => res.json()).catch(console.error);
        if (newCursor == undefined) {
          throw new Error("newCursor is undefined");
        }
        setCursor(newCursor);
        setHasMore(newCursor > -1);
        setOldMessages(prev => [...prev, ...posts]);
      } catch (error) {
        console.error('Error loading more posts:', error);
      } finally {
        loadingRef.current = false;
      }
    };

    loadMorePosts();
  }, [page]);

  // Window scroll event listener for infinite scroll
  useEffect(() => {
    const onScroll = () => {
      if (!hasMore || loadingRef.current) return;
      const { scrollTop, scrollHeight, clientHeight } = document.documentElement;
      if (scrollTop + clientHeight >= scrollHeight - 200) {
        setPage((prevPage) => prevPage + 1);
      }
    };

    window.addEventListener('scroll', onScroll);
    return () => window.removeEventListener('scroll', onScroll);
  }, []);

  if (initialLoading || loadingRef.current && oldMessages.length === 0 && newMessages.length === 0) {
    return <TweetSkeleton />;
  }

  if (!initialLoading && !loadingRef.current && oldMessages.length === 0 && newMessages.length === 0) {
    return <div className="flex flex-row font-ui justify-center py-20 mb-48">
      <div>
        <p>No posts yet... Make your first post!</p>
      </div>
    </div>;
  }

  return (
    <div>
      <AnimatePresence>
        {newMessages.map((tweet, i) => (
          <motion.li
            key={tweet.id}
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
          >
            <Tweet tweet={tweet} />
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
