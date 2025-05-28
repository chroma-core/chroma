import { motion } from "framer-motion";
import { AiOutlineUser } from "react-icons/ai";
import { AiOutlineRobot } from "react-icons/ai";

import { remark } from "remark";

import { useEffect, useState } from "react";
import { PostModel, Role } from "@/types";
import { getPostById } from "@/actions";
import { remarkCustom } from "@/util";
import remarkHtml from "remark-html";

import styles from "./tweet.module.css";
import { useAnimatedText } from "./animated-text";
import { randomBytes, randomInt } from "crypto";

const iconSize = 20;

interface TweetProps {
  tweet: PostModel;
  animate?: boolean;
}

export function Tweet({ tweet, animate }: TweetProps) {
  const [reply, setReply] = useState<PostModel | null>(null);

  const hasReply = tweet.replyId !== undefined && tweet.replyId !== "";

  useEffect(() => {
    if (tweet.replyId !== undefined && tweet.replyId != "") {
      getPostById(tweet.replyId).then((post) => {
        setReply((_) => post);
      });
    }
  }, []);

  const text = animate ? useAnimatedText(reply?.body ?? "") : reply?.body;

  return (
    <motion.div
      className={`flex flex-col gap-3 ${
        hasReply ? "border rounded-lg border-zinc-300 my-1 p-4" : ""
      }`}
    >
      <TweetInner role={tweet.role} body={tweet.body} />
      {hasReply ? (
        <div className="pl-8">
          <TweetInner
            className="text-[90%]"
            role={"assistant"}
            body={
              !reply
                ? "Remembering..."
                : reply.status == "error"
                ? "[Error]"
                : text ?? ""
            }
          />
        </div>
      ) : (
        ""
      )}
    </motion.div>
  );
}

function TweetInner({
  role,
  body,
  className,
}: {
  role: Role;
  body: string;
  className?: string;
}) {
  const [rendering, setRendering] = useState(true);
  const [htmlBody, setHtmlBody] = useState(body);

  const icon =
    role === "user" ? (
      <AiOutlineUser size={iconSize} />
    ) : (
      <AiOutlineRobot size={iconSize} />
    );

  useEffect(() => {
    remark()
      .use(remarkHtml)
      .use(remarkCustom)
      .process(body)
      .then((result) => {
        setRendering(false);
        setHtmlBody(result.toString());
      });
  }, [body]);

  return (
    <div className="w-full flex flex-row gap-4">
      <div className="pt-[.05em]">{icon}</div>
      {rendering ? (
        <div className="flex flex-col w-full items-stretch gap-2">
          <div className="h-4 bg-gray-300 rounded-full animate-pulse"></div>
          <div className="h-4 bg-gray-300 rounded-full animate-pulse mr-5"></div>
        </div>
      ) : (
        <div
          className={`w-full ${styles.tweetBody} ${className}`}
          dangerouslySetInnerHTML={{ __html: htmlBody }}
        ></div>
      )}
    </div>
  );
}
export function TweetSkeleton() {
  return (
    <div className="w-full flex flex-row gap-4">
      <div className="pt-[.2em]">
        <AiOutlineUser size={iconSize} />
      </div>
      <div className="flex flex-col w-full items-stretch gap-2">
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-1`} />
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-2`} />
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-4`} />
        <div className={`h-4 bg-gray-300 rounded-full animate-pulse mr-1`} />
      </div>
    </div>
  );
}
