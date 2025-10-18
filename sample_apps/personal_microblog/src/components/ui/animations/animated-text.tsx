"use client";

// Source: https://buildui.com/recipes/use-animated-text

import { animate } from "framer-motion";
import { useEffect, useState } from "react";

const delimiter = ""; // or " " to split by word

/**
 *
 * @param text
 * @returns
 */
export function useAnimatedText(text: string) {
  const [cursor, setCursor] = useState(0);
  const [startingCursor, setStartingCursor] = useState(0);
  const [prevText, setPrevText] = useState(text);

  useEffect(() => {
    const controls = animate(startingCursor, text.split(delimiter).length, {
      // Tweak the animation here
      duration: 0.1,
      ease: "easeOut",
      onUpdate(latest) {
        setCursor(Math.floor(latest));
      },
    });

    return () => controls.stop();
  }, [startingCursor, text]);

  if (prevText !== text) {
    setPrevText(text);
    setStartingCursor(cursor);
  }

  return text.split(delimiter).slice(0, cursor).join(delimiter);
}
