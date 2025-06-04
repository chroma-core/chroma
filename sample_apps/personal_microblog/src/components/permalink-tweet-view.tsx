"use client";

import { publishNewUserPost } from "@/actions";
import { TweetModel } from "@/types";
import { unixTimestampNow } from "@/util";
import { useState } from "react";
import { MarkdownContent } from "./markdown-content";
import TweetPrompt from "./tweet-prompt";
import { Tweet } from "./tweet";
import { BiReply, BiLinkAlt } from "react-icons/bi";
import { AnimatePresence, motion } from "framer-motion";
import { useRouter } from 'next/navigation'

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
          <TweetPrompt placeholder="Continue your thoughts..." onSubmit={handleSubmit} animate={false} />
          <div className='h-12' />
          {replies.map((r) => (
            <PermalinkReply key={r.id} reply={r} />
          ))}
        </div>
        {relatedPosts.length > 0 && (
          <>
            <h2 className="ml-[15px] font-ui pb-4 pt-6">Related Posts</h2>
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

function PermalinkReply({ reply }: { reply: TweetModel }) {
  const [isHovered, setIsHovered] = useState(false);
  const [isReplying, setIsReplying] = useState(false);

  // This doesn't hold all historical replies -- just the ones the user made on this page session.
  const [replies, setReplies] = useState<TweetModel[]>([]);

  const router = useRouter()

  function handleSubmit(input: string) {
    publishNewUserPost(input, reply.id).then(({ userPost }) => {
      setReplies([...replies, userPost]);
    });
  }

  return (
    <div
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      className="border-b"
    >
      <div className="pt-4 px-4 bg-[#fafafa] cursor-pointer" onClick={() => router.push(`/post/${reply.id}`)}>
        <MarkdownContent content={reply.body} className={`font-body ${reply.role === "assistant" ? "bold text-blue-600" : ""}`} />
        <motion.div
          className="flex flex-row items-center gap-2 py-2 w-full"
          initial={{ opacity: 0 }}
          animate={{ opacity: isHovered ? 1 : 0 }}
          transition={{ duration: 0.2, ease: "easeInOut" }}
        >
          <PermalinkReplyButton icon={<BiReply className={`w-5 h-5`} />} onClick={(e) => {setIsReplying((prev) => !prev); e.stopPropagation()}} />
          <PermalinkReplyButton icon={<BiLinkAlt className={`w-[18px] h-[18px]`} />} onClick={(e) => {setIsReplying((prev) => !prev); e.stopPropagation()}} />
        </motion.div>
        <div className="flex flex-row items-center gap-2">
          {replies.map((r) => (
            <div key={r.id} className="flex flex-row items-center gap-2 py-2">
              <MarkdownContent content={r.body} className={`font-body ${r.role === "assistant" ? "bold text-blue-600" : ""}`} />
            </div>
          ))}
        </div>
      </div>
      <AnimatePresence>
      {isReplying && (
        <motion.div
          initial={{ opacity: 0, height: 0}}
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
