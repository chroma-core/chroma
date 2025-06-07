"use client";

import { TweetModel } from "@/types";
import { formatDate } from "@/util";
import { readStreamableValue, StreamableValue } from "ai/rsc";
import { useEffect, useState } from "react";
import { AnimatePresence, motion } from "framer-motion";

export default function Citations({ citationIds, citationStream, collapsedByDefault = false }: { citationIds?: string[], citationStream?: StreamableValue<string, any>, collapsedByDefault?: boolean }) {
  if (citationIds == undefined && citationStream == undefined) {
    throw new Error("Either citations or citationStream must be provided");
  } else if (citationIds && citationStream) {
    throw new Error("Only one of citations or citationStream must be provided");
  }

  const [internalCitationIds, setInternalCitationIds] = useState<string[]>(citationIds ?? []);
  const [collapsed, setCollapsed] = useState(collapsedByDefault);
  const [isLoading, setIsLoading] = useState(!collapsedByDefault);
  const [loadedCitations, setLoadedCitations] = useState<TweetModel[]>([]);

  useEffect(() => {
    async function loadCitationsFromStream() {
      if (!citationStream) {
        return;
      }
      setIsLoading(true);
      try {
        for await (const citationId of readStreamableValue(citationStream)) {
          if (citationId) {
            setInternalCitationIds((prev) => [...prev, citationId]);
          }
        }
      } catch (error) {
        console.error('Streaming error:', error);
      }
      setIsLoading(false);
    }
    loadCitationsFromStream();
  }, [citationStream]);

  async function prefetchCitations() {
    if (isLoading || loadedCitations.length > 0) {
      return;
    }
    await Promise.all(internalCitationIds.map(async (id) => {
      const res = await fetch(`/api/post/${id}`);
      const json = await res.json();
      return json;
    })).then((citations) => {
      setLoadedCitations(citations);
    });
  }

  useEffect(() => {
    if (collapsed) {
      return;
    }
    setIsLoading(true);
    prefetchCitations();
    setIsLoading(false);
  }, [collapsed, internalCitationIds]);

  if (internalCitationIds.length === 0) {
    return null;
  }

  /*
   * citation-count-element and citation-list-element are in the same grid cell
   * to make them render on top of each other. This eliminates jank when switching
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
          onHoverStart={() => prefetchCitations()}
          className={`cursor-pointer col-[1] row-[1]`}>Show {internalCitationIds.length} citations</motion.div>
        {!collapsed && (
          <motion.div
            key="citation-list-element"
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            className="flex flex-col gap-2 col-[1] row-[1]">
            {loadedCitations.map((citation) => (
              <Citation key={citation.id} tweet={citation} />
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

function Citation({ tweet }: { tweet: TweetModel }) {
  const snippet = tweet.body?.split('\n')[0]?.slice(0, 100);
  const hasMore = snippet?.length && snippet.length < (tweet.body?.length ?? 0);
  return (
    <motion.div
      className="border p-2"
      initial={{ opacity: 0, height: 0 }}
      animate={{ opacity: 1, height: 'auto' }}
    >
      <div>({formatDate(tweet.date)}) {snippet}{hasMore && '...'}</div>
    </motion.div>
  );
}
