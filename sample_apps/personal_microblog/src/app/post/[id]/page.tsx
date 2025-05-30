
"use client";

import { useParams } from "next/navigation";
import { getPostById, getPostReplies, publishNewUserPost, semanticSearch } from "@/actions";
import { Tweet } from "@/components/tweet";
import TweetPrompt from "@/components/tweet-prompt";
import { TweetModel } from "@/types";
import { notFound } from "next/navigation";
import { useEffect } from "react";
import { useState } from "react";
import MarkdownContent from "@/components/markdown-content";
import { unixTimestampNow } from "@/util";
import { motion } from "framer-motion";

export default function PostPermalinkPage() {
  const params = useParams();
  const id = params.id as string;
  const [post, setPost] = useState<TweetModel | null>(null);
  const [replies, setReplies] = useState<TweetModel[]>([]);
  const [relatedPosts, setRelatedPosts] = useState<TweetModel[]>([]);

  useEffect(() => {
    getPostById(id).then(setPost);
  }, [id]);

  useEffect(() => {
    if (post) {
      getPostReplies(post.id).then(setReplies);
    }
  }, [post]);

  useEffect(() => {
    if (post) {
      semanticSearch(post.body).then((posts) => {
        setRelatedPosts(posts.filter((p) => p.id !== post.id));
      });
    }
  }, [post]);

  function handleSubmit(input: string) {
    if (!post) {
      return;
    }
    publishNewUserPost(input, post.id);
    const reply = {
      id: crypto.randomUUID(),
      threadParentId: post.id,
      role: "user",
      body: input,
      date: unixTimestampNow(),
    } as TweetModel;
    setReplies([...replies, reply]);
  }

  if (!post) {
    return <div>Loading...</div>;
  }

  return (
    <div className="flex flex-col items-center py-20">
      <div className="w-[500px] max-w-[calc(100dvw-32px)]">
        <div className="flex flex-row justify-between">
          <a href="/">⬅︎ Post</a>
          <div className="flex flex-row gap-2">
            {post.threadParentId && <a href={`/post/${post.threadParentId}`}>Parent</a>}
            <a href={`/post/${post.id}`}>Permalink</a>
          </div>
        </div>
        <div className="pt-4 pb-6">
          <div className="bg-[#fafafa] p-5">
            <div className="mb-4">
              <MarkdownContent content={post.body} />
            </div>
            {replies.map((r) => (
              <div key={r.id} className="p-4 border-l">
                <MarkdownContent content={r.body} />
              </div>
            ))}
          </div>
          <TweetPrompt placeholder="Continue your thoughts..." onSubmit={handleSubmit} />
        </div>
        <h2>Related Posts</h2>
        {relatedPosts.length > 0 ? (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.3 }}>
            {relatedPosts.map((p) => (
              <Tweet key={p.id} tweet={p} animate={false} />
            ))}
          </motion.div>
        ) : (
          <div className="text-zinc-500">Loading...</div>
        )}
      </div>
    </div>
  );
}
