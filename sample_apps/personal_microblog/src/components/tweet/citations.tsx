"use client";

import { TweetModelBase } from "@/types";
import { formatDate } from "@/util";
import { useEffect, useState, useRef, useCallback } from "react";
import { AnimatePresence, motion } from "framer-motion";
import AnimatedNumber from "../ui/animations/animated-number";
import { useRouter } from "next/navigation";

interface CitationsProps {
  citations: string[] | TweetModelBase[];
  animate?: boolean;
  collapsedByDefault?: boolean;
  className?: string;
}

export default function Citations({ citations, animate = true, collapsedByDefault = false, className = "" }: CitationsProps) {
  const onlyProvidedIds = Array.isArray(citations) && citations.every((citation) => typeof citation === 'string');
  const [collapsed, setCollapsed] = useState(collapsedByDefault);
  const [loadedCitations, setLoadedCitations] = useState<TweetModelBase[]>(onlyProvidedIds ? [] : citations);
  const isLoadingRef = useRef(false);

  const fetchCitations = useCallback(async () => {
    if (!onlyProvidedIds || isLoadingRef.current || loadedCitations.length >= citations.length) {
      return;
    }

    // Set loading immediately to prevent race conditions
    isLoadingRef.current = true;

    try {
      const idsToFetch = citations.slice(loadedCitations.length);
      const fetchedCitations = await Promise.all(idsToFetch.map(async (id) => {
        const res = await fetch(`/api/post/${id}`);
        const json = await res.json();
        return json;
      }));

      // Use functional update to prevent duplicates
      setLoadedCitations((prev) => {
        const existingIds = new Set(prev.map(citation => citation.id));
        const newCitations = fetchedCitations.filter(citation => !existingIds.has(citation.id));
        return [...prev, ...newCitations];
      });
    } finally {
      isLoadingRef.current = false;
    }
  }, [onlyProvidedIds, citations, loadedCitations.length]);

  useEffect(() => {
    if (collapsed) {
      return;
    }
    fetchCitations();
  }, [collapsed, citations]);

  if (citations.length === 0) {
    return null;
  }

  let citationsComponent = undefined;
  if (onlyProvidedIds) {
    citationsComponent = <>Show {citations.length > 0 && <AnimatedNumber number={citations.length} />} citation{citations.length === 1 ? '' : 's'}</>;
  } else {
    citationsComponent = `Show ${citations.length} citation${citations.length === 1 ? '' : 's'}`;
  }

  let router = useRouter();

  const animateCitationProps = (index: number) => animate ? {
    initial: { opacity: 0, y: 10, transition: { delay: index * 0.1 } },
    animate: { opacity: 1, y: 0, transition: { delay: index * 0.1 } },
    whileHover: { scale: 1.02, y: -2, transition: { duration: 0.2, ease: "easeOut" } },
  } : {};

/*
 * Initially, when switching the state of `collapsed`, this element would make a jank motion
 * because its height would potentially suddenly change from 1em to 0, then to something large.
 * citation-count-element and citation-list-element are in the same grid cell
 * to make them render on top of each other. This eliminates the jank when switching
 * between the two elements.
 * This effectively makes the height of the container max(citation-count-element, citation-list-element),
 * which makes for a smoother transition when `collapsed` is toggled.
 */
return (
  <div className={`grid grid-cols-1 grid-rows-1 ${className}`}>
    <AnimatePresence>
      <motion.div
        key="citation-count-element"
        initial={{ opacity: 0, height: 0 }}
        animate={{ opacity: collapsed ? 1 : 0, height: 'auto' }}
        onClick={(e) => {
          e.stopPropagation();
          setCollapsed(false);
        }}
        onHoverStart={() => collapsed && fetchCitations()}
        className={`cursor-pointer col-[1] row-[1] z-10`}>{citationsComponent}</motion.div>
      {!collapsed && (
        <motion.div
          key="citation-list-element"
          initial={{ opacity: 0, height: 0 }}
          animate={{ opacity: 1, height: 'auto' }}
          className="flex flex-col col-[1] row-[1] z-20"
        >
          {loadedCitations.map((citation, index) => (
            <motion.div key={citation.id} onClick={() => { router.push(`/post/${citation.id}`) }}
              {...animateCitationProps(index)}
              whileHover={{
                scale: 1.02,
                y: -2,
                transition: { duration: 0.2, ease: "easeOut" }
              }}>
              <Citation key={citation.id} tweet={citation} />
            </motion.div>
          ))}
        </motion.div>
      )}
    </AnimatePresence>
  </div>
);
}

function Citation({ tweet }: { tweet: TweetModelBase }) {
  const cleanedBody = tweet.body?.replace(/(!?\[.*?\])\(.*?\)/g, '$1(...)');
  const snippetLines = cleanedBody?.split('\n');
  const snippet = snippetLines?.[0];
  const hasMore = snippetLines?.length && snippetLines.length > 1;
  return (
    <div className="p-2 cursor-pointer">
      <div className="line-clamp-1">{snippet}{hasMore && '...'}</div>
    </div>
  );
}
