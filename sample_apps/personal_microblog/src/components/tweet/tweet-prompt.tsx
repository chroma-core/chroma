"use client";

import { useRef, useState, useEffect, forwardRef } from "react";
import { motion } from "framer-motion";
import styles from "./tweet-prompt.module.css";
import React from "react";

const DEBUG_MODE = false;

const InputWithSyntaxHighlighting = forwardRef<HTMLTextAreaElement, { input: string, setInput: (input: string) => void, onKeyDown: (event: React.KeyboardEvent<HTMLSpanElement>) => void, placeholder?: string }>(
  ({ input, setInput, onKeyDown, placeholder }, ref) => {
    const highlightRef = useRef<HTMLDivElement>(null);
    const autoCompleteRef = useRef<HTMLDivElement>(null);
    const textareaRef = useRef<HTMLTextAreaElement>(null);

    // The total auto-completed text and the auto-completion suffix are redundant.
    // autocomplete can easily be derived from input + autoCompletion, but autoComplete
    // is its own state to make the animation not jank.
    // With input + autoCompletion, it may be inconsistent for a fraction of a second
    // (@ass + stant), but autoComplete is always consistent.
    const [autoComplete, setAutoComplete] = useState<string>(input); // = input + autoCompletion
    const [autoCompletion, setAutoCompletion] = useState<string>("");

    function getHighlightedText(text: string) {
      const esc = (str: string) => str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      return esc(text)
        .replace(/(@assistant)($|\s|&nbsp;)/g, `<span class="text-[var(--accent)] bg-[var(--accent-secondary)]">$1</span>$2`)
        .replace(/\n/g, '<br />');
    }

    useEffect(() => {
      if (highlightRef.current && textareaRef.current) {
        highlightRef.current.innerHTML = getHighlightedText(input) + "\u200b";
        textareaRef.current.style.height = highlightRef.current.offsetHeight + "px";
      }

      // Check if input ends with any prefix of "@assistant"
      const target = "@assistant ";
      let completion = "";

      for (let i = 1; i <= target.length; i++) {
        const prefix = target.substring(0, i);
        if (input.endsWith(prefix)) {
          completion = target.substring(i);
          break;
        }
      }

      if (completion) {
        setAutoComplete(input + completion);
        setAutoCompletion(completion);
      } else {
        setAutoComplete(input);
        setAutoCompletion("");
      }
    }, [input]);

    useEffect(() => {
      if (ref && typeof ref === "object" && ref !== null) {
        (ref as React.RefObject<HTMLTextAreaElement | null>).current = textareaRef.current;
      }
    }, [ref]);

    function handleKeyDown(event: React.KeyboardEvent<HTMLTextAreaElement>) {
      if (event.key === "Tab") {
        event.preventDefault();
        setInput(input + autoCompletion);
      }
      onKeyDown(event);
    }

    return (
      <div className="relative w-full">
        <div
          ref={autoCompleteRef}
          aria-hidden="true"
          className="absolute top-0 left-0 w-full min-h-[2.5em] whitespace-pre-wrap break-words pointer-events-none z-10 font-inherit p-2 bg-transparent border border-transparent rounded-md select-none text-[var(--foreground)] opacity-50"
          dangerouslySetInnerHTML={{ __html: autoComplete }}
        />
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
          tabIndex={0}
          placeholder={placeholder ?? "What's happening?"}
          rows={1}
          onKeyDown={handleKeyDown}
        />
      </div>
    );
  }
);

interface TweetPromptProps {
  onSubmit: (input: string) => void;
  placeholder?: string;
  animate?: boolean;
}

export default function TweetPrompt(props: TweetPromptProps) {
  const animate = props.animate ?? true;

  const [glow, setGlow] = useState<boolean>(false);
  const [input, setInput] = useState<string>("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    setGlow(input.match(/(^|\s|&nbsp;)@assistant($|\s|&nbsp;)/) != null);
  }, [input]);

  function handleSubmit() {
    // Do nothing if input is empty
    if (input.trim() === "") {
      return;
    }
    const userInput = input;
    setInput("");
    try {
      props.onSubmit(userInput);
    } catch (error) {
      setInput(userInput);
    }
  }

  function handleKeyDown(event: React.KeyboardEvent<HTMLSpanElement>) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      handleSubmit();
    }
  }
  const outlineVariants = {
    backgroundSize: glow ? "200%" : 0,
    y: glow ? -2 : 0,
  };

  const visibilityVariants = {
    opacity: input.length > 5 ? 1 : 0,
  };

  function handleContainerClick() {
    textareaRef.current?.focus();
  }

  const allowedToSubmit = input.length > 0;

  return (
    <motion.div
      animate={animate ? outlineVariants : {}}
      transition={{
        duration: 0.3,
      }}
      className={`cursor-text border rounded-sm ${glow ? animate ? styles.shadow : " border-[var(--accent)]" : "border-zinc-100"}`}
      onClick={handleContainerClick}
    >
      <div className={`font-ui flex flex-col gap-2 relative items-end w-full px-2 py-1.5 w-full outline-none text-zinc-800 bg-[var(--background-secondary)] ${DEBUG_MODE ? styles.debugLayers : ""}`}>
        <InputWithSyntaxHighlighting ref={textareaRef} input={input} setInput={setInput} onKeyDown={handleKeyDown} placeholder={props.placeholder} />
        <div className="flex flex-row gap-1">
          <motion.div className="px-2 py-1 text-xs text-zinc-500 opacity-0" animate={visibilityVariants}>{input.length}</motion.div>
          <button
            className={`px-2 py-1 text-xs text-zinc-500 rounded-sm ${allowedToSubmit ? "text-zinc-800 hover:bg-zinc-200" : "cursor-not-allowed"}`}
            onClick={allowedToSubmit ? handleSubmit : undefined}
          >Send</button>
        </div>
      </div>
    </motion.div>
  );
}
