"use client";

import { publishNewUserPost } from "@/actions";
import { EnrichedTweetModel, NewPostResponseTweetModel, TweetModelBase, UserWithStreamingAIResponseTweetModel } from "@/types";
import { useEffect, useState } from "react";
import TweetPrompt from "../tweet/tweet-prompt";
import { Tweet } from "../tweet/tweet";
import { useRouter } from 'next/navigation'
import TweetBody from "../tweet/tweet-body";
import { formatDate } from "@/util";
import { motion } from "framer-motion";

interface PermalinkTweetViewProps {
  post: TweetModelBase;
  parentPosts: TweetModelBase[];
  existingReplies: EnrichedTweetModel[];
}

export default function PermalinkTweetView({ post, parentPosts, existingReplies }: PermalinkTweetViewProps) {
  const [newReplies, setNewReplies] = useState<NewPostResponseTweetModel[]>([]);

  function handleSubmit(input: string) {
    const postReply = async () => {
      if (!post) {
        return;
      }
      const reply: NewPostResponseTweetModel = await publishNewUserPost(input, post.id);
      setNewReplies([...newReplies, reply]);
    }
    postReply();
  }

  function webNativeShare() {
    const url = window.location.href;
    const data = {
      title: post.body,
      text: post.body,
      url: url,
    }
    if (!navigator.canShare(data)) {
      return;
    }
    navigator.share(data).catch(() => {
      // Do nothing
    });
  }

  const omitIds = [post.id, ...existingReplies.map((r) => r.id), ...newReplies.flatMap((r) => r.aiReplyId ? [r.id, r.aiReplyId] : [r.id])];

  const headerComponent = (<div className="flex flex-row font-ui justify-between sticky top-0 bg-[var(--background)] py-4">
    <a href="/">← Feed</a>
    <div className="flex flex-row gap-2">
      <button onClick={webNativeShare}>Share</button>
    </div>
  </div>);

  const isAiReply = post.role === "assistant";

  return (
    <>
      <div className="py-4">
        <div>
          {headerComponent}
          <div className="pb-12 px-4 pt-4">
            <div className="pt-4">
              <ParentPosts parentPosts={parentPosts} />
            </div>
            {post.date && <div className="text-sm text-gray-500 pb-2">{formatDate(post.date)}</div>}
            <TweetBody
              body={post.body}
              citations={post.citations}
              className="text-[1.15em]/7 font-body"
              bodyProps={{ className: isAiReply ? "text-[var(--accent)]" : "" }}
              citationsProps={{ className: "text-[var(--accent)] ml-2", animate: false, style: isAiReply ? "endnote" : "block" }}
            />
          </div>
        </div>
        <TweetPrompt placeholder="Continue your thoughts..." onSubmit={handleSubmit} animate={false} />
        <div className='h-12' />
        {existingReplies.map((r) => (
          <PermalinkReply key={r.id} reply={r} />
        ))}
        {newReplies.map((r) => (
          <NewReply key={r.id} reply={r} />
        ))}
      </div>
      <div className="min-h-[100dvh]">
        <RelatedPosts searchTerm={post.body} omitIds={omitIds} />
      </div>
    </>
  );
}

function ParentPosts({ parentPosts }: { parentPosts: TweetModelBase[] }) {
  if (parentPosts.length === 0) {
    return null;
  }

  const router = useRouter();

  function goToPostPage(id: string) {
    router.push(`/post/${id}`);
  }

  return (
    <div className="pb-6">
      {parentPosts.map((p) => (
        <div key={p.id} className="grid grid-cols-[2px_auto] last:pb-8 w-full border-l cursor-pointer" onClick={() => goToPostPage(p.id)}>
          <div className="border-l-[1.5px] border-gray-400 h-[1.2em] mt-2 ml-[-1.5px]"></div>
          <div className="p-2 pl-4 pb-4">
            <div className="text-sm text-gray-500 pb-2">{formatDate(p.date)}</div>
            <TweetBody body={p.body} citations={p.citations} className="opacity-70" />
          </div>
        </div>
      ))}
    </div>
  );
}

function PermalinkReply({ reply }: { reply: EnrichedTweetModel }) {
  const router = useRouter()

  return (
    <div className="cursor-pointer" onClick={() => router.push(`/post/${reply.id}`)}>
      <Tweet tweet={reply} />
    </div>
  );
}

function NewReply({ reply }: { reply: NewPostResponseTweetModel }) {
  const router = useRouter();
  return (
    <div className="cursor-pointer" onClick={() => router.push(`/post/${reply.id}`)}>
      <Tweet tweet={reply} />
    </div>
  );
}

function RelatedPosts({ searchTerm, omitIds }: { searchTerm: string, omitIds: string[] }) {
  const [relatedPosts, setRelatedPosts] = useState<EnrichedTweetModel[]>([]);

  useEffect(() => {
    const getRelatedPosts = async () => {
      const response = await fetch('/api/search', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: searchTerm
        })
      });
      const relatedPosts = await response.json();
      setRelatedPosts(
        relatedPosts
        .filter((p: EnrichedTweetModel) => !omitIds.includes(p.id))
        .sort((a: EnrichedTweetModel, b: EnrichedTweetModel) => b.date - a.date)
        .slice(0, 5));
    };
    getRelatedPosts();
  }, [searchTerm, omitIds]);

  if (relatedPosts.length === 0) {
    return null;
  }

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.1, ease: "easeOut" }}
    >
      <h2 className="ml-[15px] font-ui pb-4 pt-6">Related Posts</h2>
      <div>
        {relatedPosts.map((p) => (
          <Tweet key={p.id} tweet={p} />
        ))}
      </div>
    </motion.div>
  );
}
