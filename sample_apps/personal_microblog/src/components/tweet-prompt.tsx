import { useRef, useState, useEffect, forwardRef } from "react";
import { motion } from "framer-motion";
import styles from "./tweet-prompt.module.css";
import React from "react";

const InputWithSyntaxHighlighting = forwardRef<HTMLTextAreaElement, { input: string, setInput: (input: string) => void, onKeyDown: (event: React.KeyboardEvent<HTMLSpanElement>) => void }>(
  ({ input, setInput, onKeyDown }, ref) => {
    const highlightRef = useRef<HTMLDivElement>(null);
    const textareaRef = useRef<HTMLTextAreaElement>(null);

    function getHighlightedText(text: string) {
      const esc = (str: string) => str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      return esc(text)
        .replace(/(@assistant)($|\s|&nbsp;)/g, `<span class="text-blue-600 bg-blue-100">$1</span>$2`)
        .replace(/(#[\w-]+)/g, '<span class="text-green-600">$1</span>')
        .replace(/\n/g, '<br />');
    }

    useEffect(() => {
      if (highlightRef.current && textareaRef.current) {
        highlightRef.current.innerHTML = getHighlightedText(input) + "\u200b";
        textareaRef.current.style.height = highlightRef.current.offsetHeight + "px";
      }
    }, [input]);

    useEffect(() => {
      if (ref && typeof ref === "object" && ref !== null) {
        (ref as React.RefObject<HTMLTextAreaElement | null>).current = textareaRef.current;
      }
    }, [ref]);

    return (
      <div className="relative w-full">
        <div
          ref={highlightRef}
          aria-hidden="true"
          className="absolute top-0 left-0 w-full min-h-[2.5em] whitespace-pre-wrap break-words pointer-events-none z-10 font-inherit p-2 bg-transparent border border-transparent rounded-md select-none"
          dangerouslySetInnerHTML={{ __html: getHighlightedText(input) }}
        />
        <textarea
          ref={textareaRef}
          value={input}
          onChange={e => setInput(e.target.value)}
          className="relative w-full min-h-[2.5em] resize-none bg-transparent text-transparent caret-black z-20 font-inherit p-2 outline-none overflow-hidden"
          spellCheck={true}
          autoFocus={true}
          rows={1}
          onKeyDown={onKeyDown}
        />
      </div>
    );
  }
);

interface TweetPromptProps {
  onSubmit: (input: string) => void;
}

export default function TweetPrompt(props: TweetPromptProps) {
  const [glow, setGlow] = useState<boolean>(false);
  const [input, setInput] = useState<string>("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    setGlow(input.match(/(^|\s|&nbsp;)@assistant($|\s|&nbsp;)/) != null);
  }, [input]);

  function handleKeyDown(event: React.KeyboardEvent<HTMLSpanElement>) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      // Do nothing if input is empty
      if (input.trim() === "") {
        return;
      }
      props.onSubmit(input);
      setInput("");
    }
  }
  const outlineVariants = {
    outlineWidth: glow ? 2 : 0,
    boxShadow: glow ? "0 0 0 6px #ffb4b4" : "none",
    y: glow ? -4 : 0, // This gives the input a little bounce
  };

  function handleContainerClick() {
    textareaRef.current?.focus();
  }

  return (
    <motion.div
      animate={outlineVariants}
      transition={{
        duration: 0.2,
        y: {
          type: "spring",
          stiffness: 300,
          damping: 15,
        },
      }}
      className={`flex flex-col gap-2 relative items-end w-full bg-zinc-100 rounded-md px-2 py-1.5 w-full outline-none text-zinc-800 cursor-text`}
      onClick={handleContainerClick}
    >
      <InputWithSyntaxHighlighting ref={textareaRef} input={input} setInput={setInput} onKeyDown={handleKeyDown} />
      <div className="px-2 py-1 text-xs text-zinc-500">{input.length}</div>
    </motion.div>
  );
}
