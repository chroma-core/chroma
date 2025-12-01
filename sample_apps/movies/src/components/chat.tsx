"use client";

import { Input } from "@/components/ui/input";
import { useChat } from "@ai-sdk/react";
import { SearchResultRow } from "chromadb";
import { ChevronUpIcon, ChevronDownIcon, LoaderCircleIcon } from "lucide-react";
import { useEffect, useRef, useState } from "react";

export default function Chat() {
  const [input, setInput] = useState("");
  const { messages, sendMessage, status } = useChat();

  const bottomRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!messages.length) return;
    // Scroll to the bottom whenever messages update
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  return (
    <div className="flex flex-1 flex-col overflow-hidden px-2 pt-4">
      <div className="mx-auto w-full max-w-2xl space-y-4 overflow-y-auto px-1 pb-28">
        {messages.map((m) => (
          <div key={m.id} className="whitespace-pre-wrap">
            <div>
              <div className="font-bold">{m.role}</div>
              {m.parts.map((part, index) => {
                switch (part.type) {
                  case "text":
                    return <p key={`part-${index}`}>{part.text}</p>;
                  case "data-context":
                    return (
                      <ContextRow
                        key={`part-${index}`}
                        context={part.data as ContextItem}
                      />
                    );
                }
              })}
            </div>
          </div>
        ))}

        {(status === "submitted" || status === "streaming") && (
          <div className="flex flex-col items-center py-2">
            <LoaderCircleIcon className="size-5 animate-spin" />
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      <form
        onSubmit={(e) => {
          e.preventDefault();
          sendMessage({ text: input });
          setInput("");
        }}
      >
        <Input
          className="fixed bottom-0 left-1/2 mb-8 w-full max-w-md -translate-x-1/2 transform bg-white shadow-xl"
          value={input}
          placeholder="Chat about movies..."
          onChange={(e) => setInput(e.currentTarget.value)}
        />
      </form>
    </div>
  );
}

type ContextItem = {
  documents: SearchResultRow[];
};

function ContextRow({ context }: { context: ContextItem }) {
  const [isOpen, setIsOpen] = useState(false);

  if (!context.documents.length) {
    return null;
  }

  return (
    <>
      <button
        className="my-2 flex w-full cursor-pointer flex-row items-center justify-between rounded-sm border px-4 py-0.5 text-sm shadow-sm"
        onClick={() => setIsOpen(!isOpen)}
      >
        <span>Chroma context: {context.documents.length} items</span>

        {isOpen ? (
          <ChevronUpIcon className="size-4" />
        ) : (
          <ChevronDownIcon className="size-4" />
        )}
      </button>

      {isOpen && (
        <div className="mb-6 px-4">
          {context.documents.map((doc) => {
            return (
              <div key={doc.id} className="border-b py-4 text-sm">
                <span className="font-semibold">{doc.id}</span> {doc.document}
                {doc.metadata && Object.keys(doc.metadata).length > 0 && (
                  <div className="mt-2 flex flex-wrap gap-1.5">
                    {Object.entries(doc.metadata)
                      .filter(([key]) => key !== "bm25_sparse_vector")
                      .map(([key, value]) => (
                        <span
                          key={key}
                          className="bg-muted text-muted-foreground inline-flex items-center rounded-full px-2 py-0.5 text-xs"
                        >
                          <span className="font-medium">{key}:</span>
                          <span className="ml-1 max-w-[150px] truncate">
                            {String(value)}
                          </span>
                        </span>
                      ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </>
  );
}
