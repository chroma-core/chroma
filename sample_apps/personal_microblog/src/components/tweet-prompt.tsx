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
        .replace(/(@assistant)($|\s|&nbsp;)/g, `<span class="text-blue-600 bg-blue-100">$1</span>$2`)
        .replace(/(#[\w-]+)/g, '<span class="text-green-600">$1</span>')
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
          className="absolute top-0 left-0 w-full min-h-[2.5em] whitespace-pre-wrap break-words pointer-events-none z-10 font-inherit p-2 bg-transparent border border-transparent rounded-md select-none text-gray-400"
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
    backgroundSize: animate && glow ? "200%" : 0,
    y: animate && glow ? -2 : 0,
  };

  const visibilityVariants = {
    opacity: input.length > 5 ? 1 : 0,
  };

  function handleContainerClick() {
    textareaRef.current?.focus();
  }

  return (
    <motion.div
      animate={outlineVariants}
      transition={{
        duration: 0.3,
      }}
      className={`cursor-text border ${glow ? animate ? styles.shadow : " border-blue-200" : "border-zinc-100"}`}
      onClick={handleContainerClick}
    >
      <div className={`font-ui flex flex-col gap-2 relative items-end w-full bg-zinc-100 px-2 py-1.5 w-full outline-none text-zinc-800 ${DEBUG_MODE ? styles.debugLayers : ""}`}>
        <InputWithSyntaxHighlighting ref={textareaRef} input={input} setInput={setInput} onKeyDown={handleKeyDown} placeholder={props.placeholder} />
        <div className="flex flex-row gap-1">
          <motion.div className="px-2 py-1 text-xs text-zinc-500 opacity-0" animate={visibilityVariants}>{input.length}</motion.div>
          <button className={`px-2 py-1 text-xs text-zinc-500 ${input.length > 0 ? "text-zinc-800" : "cursor-not-allowed"}`} onClick={handleSubmit}>Send</button>
        </div>
      </div>
    </motion.div>
  );
}
