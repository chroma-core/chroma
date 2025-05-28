import { useRef, useState, useEffect } from "react";
import { motion } from "framer-motion";
import styles from "./tweet-prompt.module.css";
import React from "react";

function CharLimitIndicator(props: { count: number; max: number }) {
  return (
    <div className="px-2 py-1 text-xs text-zinc-500">
      {props.count}/{props.max}
    </div>
  );
}

function normalizeInput(input: string) {
  return input.replace("<br>", "\n\n").replace("&nbsp;", " ");
}

interface TweetPromptProps {
  onSubmit: (input: string) => void;
}

export default function TweetPrompt(props: TweetPromptProps) {
  const [glow, setGlow] = useState<boolean>(false);
  const [input, setInput] = useState<string>("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    // If you delete all content in the input area, it will be left with a <br> tag.
    // This corrects this.
    if (input === "<br>") {
      setInput("");
      if (inputRef.current) {
        inputRef.current.innerHTML = "";
      }
    }
    setGlow(
      normalizeInput(input).match(/(^|\s|&nbsp;)@assistant($|\s|&nbsp;)/) !=
        null
    );
  }, [input]);

  function handleKeyDown(event: React.KeyboardEvent<HTMLSpanElement>) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (input.trim() === "") {
        return;
      }
      props.onSubmit(normalizeInput(input));
      setInput("");
      if (inputRef.current) {
        inputRef.current.innerHTML = "";
      }
    }
  }

  const outlineVariants = {
    outlineWidth: glow ? 2 : 0,
    boxShadow: glow ? "0 0 0 4px #ffb4b4" : "none",
  };

  return (
    <motion.div
      animate={outlineVariants}
      transition={{ duration: 0.2 }}
      className={`flex flex-col gap-2 relative items-end w-full bg-zinc-100 rounded-md px-2 py-1.5 w-full outline-none text-zinc-800`}
    >
      <span
        ref={inputRef}
        className={`w-full outline-none ` + styles.divInput}
        onInput={(event) => setInput(event.currentTarget.innerHTML)}
        onKeyDown={(event) => handleKeyDown(event)}
        autoFocus={true}
        contentEditable={true}
        role="textbox"
      ></span>
      <CharLimitIndicator count={normalizeInput(input).length} max={140} />
    </motion.div>
  );
}
