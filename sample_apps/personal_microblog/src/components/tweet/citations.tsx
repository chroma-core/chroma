"use client";

import { TweetModelBase } from "@/types";
import { formatDate } from "@/util";
import { useEffect, useState, useRef, useCallback } from "react";
import { AnimatePresence, motion } from "framer-motion";
import AnimatedNumber from "../ui/animations/animated-number";
import { useRouter } from "next/navigation";

interface CitationsProps {
  citations: string[] | TweetModelBase[];
  collapsedByDefault?: boolean;
}

export default function Citations({ citations, collapsedByDefault = false }: CitationsProps) {
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
    <div className="grid grid-cols-1 grid-rows-1">
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
            className="flex flex-col gap-2 col-[1] row-[1] z-20"
          >
            {loadedCitations.map((citation) => (
              <div key={citation.id} onClick={() => { router.push(`/post/${citation.id}`) }}>
                <Citation key={citation.id} tweet={citation} />
              </div>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function Citation({ tweet }: { tweet: TweetModelBase }) {
  const snippet = tweet.body?.replace(/(!?\[.*?\])\(.*?\)/g, '$1(...)').split('\n')[0]?.slice(0, 100);
  const hasMore = snippet?.length && snippet.length < (tweet.body?.length ?? 0);
  return (
    <motion.div
      className="border p-2"
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
    >
      <div>{snippet}{hasMore && '...'}</div>
    </motion.div>
  );
}
