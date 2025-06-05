"use client";

import { publishNewUserPost, semanticSearch } from "@/actions";
import { TweetModel } from "@/types";
import { unixTimestampNow } from "@/util";
import { useEffect, useState } from "react";
import { MarkdownContent } from "./markdown-content";
import TweetPrompt from "./tweet-prompt";
import { Tweet } from "./tweet";
import { BiReply, BiLinkAlt } from "react-icons/bi";
import { AnimatePresence, motion } from "framer-motion";
import { useRouter } from 'next/navigation'

export default function PermalinkTweetView({ post, parentPosts, existingReplies }: { post: TweetModel, parentPosts: TweetModel[], existingReplies: TweetModel[] }) {
  const [replies, setReplies] = useState<TweetModel[]>(existingReplies);
  const [relatedPosts, setRelatedPosts] = useState<TweetModel[]>([]);

  useEffect(() => {
    const getRelatedPosts = async () => {
      const response = await fetch(`/api/search?q=${encodeURIComponent(post.body)}`);
      const relatedPosts = await response.json();
      setRelatedPosts(relatedPosts.filter((p: TweetModel) => p.id !== post.id));
    };
    getRelatedPosts();
  }, [post]);

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

  if (!post) {
    return <div>Loading...</div>;
  }

  return (
    <div className="flex flex-col items-center py-20">
      <div className="w-[600px] max-w-[calc(100dvw-32px)]">
        <div className="flex flex-row font-ui justify-between">
          <a href="/">← Feed</a>
          <div className="flex flex-row gap-2">
            {post.threadParentId && <a href={`/post/${post.threadParentId}`}>Parent</a>}
            <a href={`/post/${post.id}`}>Permalink</a>
          </div>
        </div>
        <div className="py-4">
          <div className="bg-[#fafafa] py-12 px-4">
            {parentPosts.map((p) => <div key={p.id} className="bg-gray-100 p-2 w-full">
              <MarkdownContent content={p.body} className="text-[1.15em]/5 font-body" />
            </div>)}
            <MarkdownContent content={post.body} className="text-[1.15em]/5 font-body" />
            <PermalinkTweetCitations citationIds={post.citations} />
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

function PermalinkTweetCitations({ citationIds }: { citationIds: string[] }) {
  const [loading, setLoading] = useState(true);
  const [citations, setCitations] = useState<TweetModel[]>([]);

  useEffect(() => {
    const getCitations = async () => {
      const results = await Promise.all(citationIds.map(async (id) => {
        const res = await fetch(`/api/post/${id}`);
        return res.json();
      }));
      console.log(results);
      setCitations(results);
      setLoading(false);
    };
    getCitations();
  }, []);
  console.log(loading, citations);
  return (
    (!loading && citations.length > 0) && (<div className="mt-4 flex flex-row gap-2">
      {citations.map((c) => <div key={c.id} className="bg-gray-100 p-2 w-full">
        <MarkdownContent content={c.body} className="text-[1.15em]/5 font-body" />
      </div>)}
    </div>)
  );
}

function PermalinkReply({ reply }: { reply: TweetModel }) {
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
        <MarkdownContent content={reply.body} className={`font-body ${reply.role === "assistant" ? "bold text-blue-600" : ""}`} />
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
