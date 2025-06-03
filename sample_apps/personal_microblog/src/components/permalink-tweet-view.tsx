"use client";

import { publishNewUserPost } from "@/actions";
import { TweetModel } from "@/types";
import { unixTimestampNow } from "@/util";
import { useState } from "react";
import MarkdownContent from "./markdown-content";
import TweetPrompt from "./tweet-prompt";
import { motion } from "framer-motion";
import { Tweet } from "./tweet";

export default function PermalinkTweetView({ post, existingReplies, relatedPosts }: { post: TweetModel, existingReplies: TweetModel[], relatedPosts: TweetModel[] }) {
  const [replies, setReplies] = useState<TweetModel[]>(existingReplies);

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
      <div className="w-[600px] max-w-[calc(100dvw-32px)]">
        <div className="flex flex-row font-ui justify-between">
          <a href="/">← Post</a>
          <div className="flex flex-row gap-2">
            {post.threadParentId && <a href={`/post/${post.threadParentId}`}>Parent</a>}
            <a href={`/post/${post.id}`}>Permalink</a>
          </div>
        </div>
        <div className="py-4">
          <div className="bg-[#fafafa] py-12 px-4">
            <MarkdownContent content={post.body} className="text-[1.15em]/5 font-body" />
          </div>
          <TweetPrompt placeholder="Continue your thoughts..." onSubmit={handleSubmit} />
          {replies.map((r) => (
            <div key={r.id} className="py-4 px-4 bg-[#fafafa] border-b">
              <MarkdownContent content={r.body} className={`font-body ${r.role === "assistant" ? "bold text-blue-600" : ""}`} />
            </div>
          ))}

        </div>
        {relatedPosts.length > 0 && (
          <>
            <h2 className="ml-[30px] font-ui pb-2">Related Posts</h2>
            <div>
              {relatedPosts.map((p) => (
                <Tweet key={p.id} tweet={p} />
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
