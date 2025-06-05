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
      try {
        for await (const citationId of readStreamableValue(citationStream)) {
          if (citationId) {
            setInternalCitationIds((prev) => [...prev, citationId]);
          }
        }
      } catch (error) {
        console.error('Streaming error:', error);
      }
    }
    loadCitationsFromStream();
  }, [citationStream]);

  useEffect(() => {
    if (collapsed) {
      return;
    }
    async function loadCitations() {
      await Promise.all(internalCitationIds.map(async (id) => {
        const res = await fetch(`/api/post/${id}`);
        const json = await res.json();
        return json;
      })).then((citations) => {
        setLoadedCitations(citations);
      });
    }
    setIsLoading(true);
    loadCitations();
    setIsLoading(false);
  }, [collapsed, internalCitationIds]);

  if (internalCitationIds.length === 0) {
    return null;
  }

  if (collapsed) {
    return (
      <div onClick={(e) => {
        e.stopPropagation();
        setCollapsed(false);
      }}
        className="cursor-pointer">Show {internalCitationIds.length} citations</div>
    )
  }

  return (
    <AnimatePresence>
      <div className="flex flex-col gap-2">
        {loadedCitations.map((citation) => (
          <Citation key={citation.id} tweet={citation} />
        ))}
      </div>
    </AnimatePresence>
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
