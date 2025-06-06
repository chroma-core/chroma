"use client";

import { publishNewUserPost } from "@/actions";
import { EnrichedTweetModel, TweetModelBase } from "@/types";
import { useEffect, useState } from "react";
import TweetPrompt from "../tweet/tweet-prompt";
import { Tweet } from "../tweet/tweet";
import { BiReply, BiLinkAlt } from "react-icons/bi";
import { AnimatePresence, motion } from "framer-motion";
import { useRouter } from 'next/navigation'
import TweetBody from "../tweet/tweet-body";

export default function PermalinkTweetView({ post, parentPosts, existingReplies }: { post: TweetModelBase, parentPosts: TweetModelBase[], existingReplies: TweetModelBase[] }) {
  const [replies, setReplies] = useState<TweetModelBase[]>(existingReplies);

  function handleSubmit(input: string) {
    const postReply = async () => {
      if (!post) {
        return;
      }
      const { userPost } = await publishNewUserPost(input, post.id);
      setReplies([...replies, userPost]);
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

  const headerComponent = (<div className="flex flex-row font-ui justify-between sticky top-0 bg-[var(--background)] py-4">
    <a href="/">← Feed</a>
    <div className="flex flex-row gap-2">
      <button onClick={webNativeShare}>Share</button>
    </div>
  </div>);

  return (
    <div className="flex flex-col items-center py-20">
      <div className="w-[600px] max-w-[calc(100dvw-32px)]">
        <div className="py-4">
          {headerComponent}
          <div className="py-12 px-4">
            <ParentPosts parentPosts={parentPosts} />
            <TweetBody body={post.body} citations={post.citations} className="text-[1.15em]/5 font-body" />
          </div>
          <TweetPrompt placeholder="Continue your thoughts..." onSubmit={handleSubmit} animate={false} />
          <div className='h-12' />
          {replies.map((r) => (
            <PermalinkReply key={r.id} reply={r} />
          ))}
        </div>
        <div className="min-h-[100dvh]">
          <RelatedPosts searchTerm={post.body} currentPostId={post.id} />
        </div>
      </div>
    </div>
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
        <div key={p.id} className="grid grid-cols-[2px_auto] last:pb-16 w-full border-l cursor-pointer" onClick={() => goToPostPage(p.id)}>
          <div className="border-l-[1.5px] border-gray-400 h-[1.2em] mt-3 ml-[-1.5px]"></div>
          <TweetBody body={p.body} citations={p.citations} className="opacity-70 p-2 pl-4 pb-4" />
        </div>
      ))}
    </div>
  );
}

function PermalinkReply({ reply }: { reply: TweetModelBase }) {
  const [isHovered, setIsHovered] = useState(false);
  const [isReplying, setIsReplying] = useState(false);

  const router = useRouter()

  function handleSubmit(input: string) {
    publishNewUserPost(input, reply.id).then(({ userPost }) => {
      router.push(`/post/${userPost.id}`);
    });
  }

  return (
    <div
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      className="border-b"
    >
      <div className="pt-4 px-4 cursor-pointer" onClick={() => router.push(`/post/${reply.id}`)}>
        <TweetBody body={reply.body} citations={reply.citations} className={`font-body ${reply.role === "assistant" ? "bold text-blue-600" : ""}`} />
        <motion.div
          className="flex flex-row items-center gap-2 py-2 w-full"
          initial={{ opacity: 0 }}
          animate={{ opacity: isHovered ? 1 : 0 }}
          transition={{ duration: 0.2, ease: "easeInOut" }}
        >
          <PermalinkReplyButton icon={<BiReply className={`w-5 h-5`} />} onClick={(e) => { setIsReplying((prev) => !prev); e.stopPropagation() }} />
          <PermalinkReplyButton icon={<BiLinkAlt className={`w-[18px] h-[18px]`} />} onClick={(e) => { setIsReplying((prev) => !prev); e.stopPropagation() }} />
        </motion.div>
      </div>
      <AnimatePresence>
        {isReplying && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            transition={{ duration: 0.2, ease: "easeInOut" }}
          >
            <TweetPrompt placeholder="Reply to this post..." onSubmit={handleSubmit} animate={false} />
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function PermalinkReplyButton({ icon, onClick }: { icon: React.ReactNode, onClick: (event: React.MouseEvent<HTMLDivElement>) => void }) {
  const [isHovered, setIsHovered] = useState(false);

  return (
    <div
      className="relative flex flex-row items-center gap-2 py-2 text-gray-500 hover:text-black overflow-hidden rounded-full"
      onClick={onClick}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <motion.div
        className="absolute inset-0 bg-gray-200 rounded-full"
        initial={{ scale: 0, opacity: 0 }}
        animate={{ scale: isHovered ? .8 : 0, opacity: isHovered ? 1 : 0 }}
        transition={{ duration: 0.2, ease: "easeInOut" }}
      />
      <div className="relative z-10">
        {icon}
      </div>
    </div>
  );
}

function RelatedPosts({ searchTerm, currentPostId }: { searchTerm: string, currentPostId: string }) {
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
      setRelatedPosts(relatedPosts.filter((p: EnrichedTweetModel) => p.id !== currentPostId));
    };
    getRelatedPosts();
  }, [searchTerm, currentPostId]);

  if (relatedPosts.length === 0) {
    return null;
  }

  return (
    <>
      <h2 className="ml-[15px] font-ui pb-4 pt-6">Related Posts</h2>
      <div>
        {relatedPosts.map((p) => (
          <Tweet key={p.id} tweet={p} aiReply={p.enrichedAiReply} />
        ))}
      </div>
    </>
  );
}
