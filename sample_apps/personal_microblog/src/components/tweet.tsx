import { motion } from "framer-motion";
import { AiOutlineUser } from "react-icons/ai";
import { AiOutlineRobot } from "react-icons/ai";

import { Mention } from "./markup";
import { JSX } from "react";
import { PostModel, Role } from "@/types";

interface TweetProps {
  tweet: PostModel;
}

export function Tweet({ tweet }: TweetProps) {
  return (
    <div className="flex flex-col gap-3">
      <TweetInner role={tweet.role} body={tweet.body} />
      <div className="pl-8">
        {tweet.reply ? (
          <TweetInner role={"assistant"} body={tweet.reply} />
        ) : (
          ""
        )}
      </div>
    </div>
  );
}

function TweetInner({ role, body }: { role: Role; body: string }) {
  const icon = role === "user" ? <AiOutlineUser /> : <AiOutlineRobot />;
  return (
    <div className="w-full flex flex-row gap-4">
      <div className="pt-[.2em]">{icon}</div>
      <div>
        <MarkedUpTweetBody body={body} />
      </div>
    </div>
  );
}

function MarkedUpTweetBody({ body }: { body: string }): JSX.Element[] {
  const parts: JSX.Element[] = [];
  const mentionRegex = /(@\w+)/g;
  let lastIndex = 0;

  body.replace(mentionRegex, (match, mention, offset) => {
    // Push text before the mention
    if (lastIndex < offset) {
      const content = body.slice(lastIndex, offset);
      parts.push(<span key={`${lastIndex}-${offset}`}>{content}</span>);
    }
    // Push the mention
    parts.push(<Mention key={offset} text={mention} />);
    lastIndex = offset + match.length;
    return "";
  });

  // Push any remaining text
  if (lastIndex < body.length) {
    const content = body.slice(lastIndex);
    parts.push(<span key={lastIndex}>{content}</span>);
  }

  return parts;
}

export function TweetSkeleton() {
  return (
    <div className="w-full flex flex-row gap-4">
      <div className="pt-[.2em]">
        <AiOutlineUser />
      </div>
      <div className="flex flex-col w-full items-stretch gap-2">
        <div className="w-full h-4 bg-gray-300 rounded-full animate-pulse"></div>
        <div className="w-full h-4 bg-gray-300 rounded-full animate-pulse"></div>
      </div>
    </div>
  );
}
