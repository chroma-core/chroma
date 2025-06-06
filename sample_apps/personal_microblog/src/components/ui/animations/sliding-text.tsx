"use client";

import { AnimatePresence, motion } from "framer-motion";
import { useEffect, useState } from "react";

interface SlidingTextProps {
  text: string;
  className?: string;
}

export default function SlidingText({ text, className = "" }: SlidingTextProps) {
  const [currentText, setCurrentText] = useState(text);
  const [key, setKey] = useState(0);

  useEffect(() => {
    if (text !== currentText) {
      setCurrentText(text);
      setKey(prev => prev + 1);
    }
  }, [text, currentText]);

  return (
    <div className={`relative overflow-hidden ${className}`}>
      <AnimatePresence mode="wait">
        <motion.div
          key={key}
          initial={{ y: 10, opacity: 0 }}
          animate={{ y: 0, opacity: 1 }}
          exit={{ y: -10, opacity: 0 }}
          transition={{
            duration: 0.15,
            ease: "easeInOut"
          }}
        >
          {currentText}
        </motion.div>
      </AnimatePresence>
    </div>
  );
}
