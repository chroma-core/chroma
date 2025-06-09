import { TweetStreamStates } from "@/types";
import { readStreamableValue, StreamableValue } from "ai/rsc";
import { useRef } from "react";
import { useEffect, useState } from "react";

export default function useTweetFromStream(
  stream: StreamableValue<string, any>
) {
  const [streamState, setStreamState] = useState<TweetStreamStates | undefined>(undefined);
  const [streamStateMessage, setStreamStateMessage] = useState<string>("Initializing...");
  const [streamedBody, setStreamedBody] = useState<string>("");
  const [streamedCitationIds, setStreamedCitationIds] = useState<string[]>([]);
  // This lock is used to prevent the stream from being read multiple times
  const readingStreamLockRef = useRef(false);

  useEffect(() => {
    async function loadDataFromStream() {
      if (!stream || readingStreamLockRef.current) {
        return;
      }
      readingStreamLockRef.current = true;
      try {
        // Make a local copy of the stream state to avoid race conditions
        // with how React handles state updates.
        let currentStreamState = streamState;
        for await (const res of readStreamableValue(stream)) {
          if (!res) {
            continue;
          }
          if (res == "--BEGIN--") {
            currentStreamState = "--BEGIN--";
          } else if (res == "--CITATIONS--") {
            currentStreamState = "--CITATIONS--";
          } else if (res == "--BODY--") {
            currentStreamState = "--BODY--";
          } else if (res == "--ERROR--") {
            currentStreamState = "--ERROR--";
          } else if (currentStreamState == "--BEGIN--") {
            setStreamStateMessage(res);
          } else if (currentStreamState == "--CITATIONS--") {
            setStreamedCitationIds((prev) => [...prev, res]);
          } else if (currentStreamState == "--BODY--") {
            setStreamedBody(res);
          } else {
            console.error('Illegal state', currentStreamState, res);
          }
          setStreamState(currentStreamState);
        }
        setStreamState("--END--");
      } catch (error) {
        console.error('Streaming error:', error);
      } finally {
        readingStreamLockRef.current = false;
      }
    }
    loadDataFromStream();
  }, [stream]);

  return {
    streamState,
    stateMessage: streamStateMessage,
    body: streamedBody,
    citations: streamedCitationIds,
  }
}
