import { readStreamableValue, StreamableValue } from "ai/rsc";
import { useEffect, useState } from "react";

export default function StreamedPost({ bodyStream, citationStream }: { bodyStream: StreamableValue<string, any>, citationStream: StreamableValue<string, any> }) {
  const [body, setBody] = useState<string>("");
  const [citations, setCitations] = useState<string[]>([]);

  useEffect(() => {
    const streamContent = async () => {
      for await (const chunk of readStreamableValue(bodyStream)) {
        setBody(prev => prev + chunk);
      }
      for await (const citation of readStreamableValue(citationStream)) {
        if (citation) {
            setCitations(prev => [...prev, citation]);
        }
      }
    };
    streamContent();
  }, [bodyStream, citationStream]);

  return <div>{body}</div>;
}
