import { motion } from "framer-motion";
import Link from "next/link";
import { TweetModel } from "./types";
import { useEffect, useState } from "react";

export const Strong: React.FC<React.HTMLProps<HTMLSpanElement>> = ({ children, ...props }) => {
  if (children === "@assistant") {
    return <span className="font-bold text-[var(--accent)]" {...props}>{children}</span>;
  }
  return <strong {...props}>{children}</strong>;
};

export const AnchorTag: React.FC = ({
  children,
  href,
}: React.HTMLProps<HTMLAnchorElement>) => {
  if (href?.startsWith("$")) {
    return <TweetReference id={href.slice(1)}>{children}</TweetReference>;
  }
  const origin = window.location.origin;
  const destination = href === undefined ? '' : href;
  const isInternalLink =
    destination.startsWith('/') || destination.startsWith(origin);
  const internalDestination = destination.replace(origin, '');
  const internalLink = (
    <Link href={internalDestination}>
      {children}
    </Link>
  );
  const externalLink = (
    <a
      href={destination}
      target="_blank"
      rel="noopener noreferrer"
    >
      {children}
    </a>
  );

  return isInternalLink ? internalLink : externalLink;
};

function TweetReference({ children, id }: { children: React.ReactNode, id: string }) {
  const [post, setPost] = useState<TweetModel | null>(null);

  useEffect(() => {
    fetch(`/api/post/${id}`).then(async (res) => {
      const json = await res.json();
      setPost(json);
    });
  }, [id]);

  let body = undefined;
  if (!post) {
    body = <span className="h-16 w-full bg-gray-100 rounded-md animate-pulse" />;
  } else {
    let date = new Date(post.date * 1000).toLocaleDateString();
    body = <motion.span
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: .4 }}
      className="w-full p-1 relative rounded-block"
    >
      <span className="bg-blue-500">{date}</span> {post.body.length > 100 ? post.body.slice(0, 100) + "..." : post.body}
    </motion.span>;
  }


  return <a href={`/post/${id}`} className="w-full no-underline bg-gray-100" style={{ display: "block", textDecoration: "none" }}>
    {body}
  </a>;
}
