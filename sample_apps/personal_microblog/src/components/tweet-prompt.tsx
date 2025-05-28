import { useRef, useState, useEffect } from "react";
import { motion } from "framer-motion";

function CharLimitIndicator(props: { count: number; max: number }) {
  return (
    <div className="px-2 py-1 text-xs text-zinc-500">
      {props.count}/{props.max}
    </div>
  );
}

interface TweetPromptProps {
  onSubmit: (input: string) => void;
}

export default function TweetPrompt(props: TweetPromptProps) {
  const [glow, setGlow] = useState<boolean>(false);
  const [input, setInput] = useState<string>("");
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setGlow(input.match("@assistant($|s)") != null);
  }, [input]);

  return (
    <div>
      <form
        className={`flex flex-col gap-2 relative items-center w-full`}
        onSubmit={async (event) => {
          event.preventDefault();
          if (input.trim() === "") {
            return;
          }
          props.onSubmit(input);
          setInput("");
        }}
      >
        <input
          ref={inputRef}
          className={`bg-zinc-100 rounded-md px-2 py-1.5 w-full outline-none text-zinc-800 ${
            glow
              ? "outline outline-2 outline-blue-400 outline-offset-2 shadow-lg shadow-blue-200"
              : ""
          }`}
          placeholder="What's happening?"
          value={input}
          onChange={(event) => {
            setInput(event.target.value);
          }}
          autoFocus={true}
        />
      </form>
      <CharLimitIndicator count={input.length} max={140} />
    </div>
  );
}
