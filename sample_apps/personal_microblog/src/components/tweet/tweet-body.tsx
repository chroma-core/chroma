import { readStreamableValue, StreamableValue } from "ai/rsc";
import { MarkdownContent, StreamedMarkdownContent } from "../ui/content/markdown-content";
import Citations from "./citations";
import SlidingText from "../ui/animations/sliding-text";
import { TweetModelBase, TweetStreamStates } from "@/types";
import { useEffect, useRef, useState } from "react";
import { useAnimatedText } from "../ui/animations/animated-text";

interface TweetBodyProps {
  body?: string;
  citations?: string[] | TweetModelBase[];
  stream?: StreamableValue<string, any>;
  className?: string;
  bodyClassName?: string;
  citationsClassName?: string;
  citationsCollapsedByDefault?: boolean;
}

export default function TweetBody({
  body,
  citations,
  stream,
  className = "",
  bodyClassName = "",
  citationsClassName = "",
  citationsCollapsedByDefault = false
}: TweetBodyProps) {
  if (body == undefined && stream == undefined) {
    throw new Error("Either body or stream must be provided");
  } else if (body && stream) {
    throw new Error("Only one of body or stream must be provided");
  }
  if (citations == undefined && stream == undefined) {
    throw new Error("Either citations or stream must be provided");
  } else if (citations && stream) {
    throw new Error("Only one of citations or stream must be provided");
  }

  const usingStream = stream != undefined;

  // These states only matter if usingStream is true, otherwise this component
  // will just use its props as its data
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

  let bodyComponent = null;
  const interpolatedText = useAnimatedText(streamedBody);
  if (!usingStream) {
    bodyComponent = <MarkdownContent content={body ?? ""} className={bodyClassName} />;
  } else if (!streamState || streamState == "--BEGIN--" || streamState == "--CITATIONS--" || streamState == "--ERROR--") {
    let message = streamStateMessage;
    if (streamState == "--ERROR--") {
      message = "Sorry, I encountered an error while processing your request.";
    }
    bodyComponent = <SlidingText text={message} className={bodyClassName} />;
  } else if (streamState == "--BODY--" || streamState == "--END--") {
    bodyComponent = <MarkdownContent content={interpolatedText} placeholder={"Generating..."} className={bodyClassName} />;
  }

  const citationsComponent = <Citations
    citations={usingStream ? streamedCitationIds : citations ?? []}
    collapsedByDefault={citationsCollapsedByDefault}
  />;

  return (
    <div className={`${className}`}>
      {bodyComponent}
      <div className="mt-2">
        {citationsComponent}
      </div>
    </div>
  );
}
