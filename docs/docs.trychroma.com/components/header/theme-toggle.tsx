"use client";

import * as React from "react";
import { Moon, Sun } from "lucide-react";
import { useTheme } from "next-themes";

import UIButton from "@/components/ui/ui-button";

const ThemeToggle = () => {
  const { theme, setTheme } = useTheme();

  const toggleTheme = () => {
    if (theme === "dark") {
      setTheme("light");
    } else {
      setTheme("dark");
    }
  };

  // make the status bar translucent on iOS
  React.useEffect(() => {
    if (!document.querySelector('meta[name="apple-mobile-web-app-status-bar-style"]')) {
      const meta = document.createElement("meta");
      meta.name = 'apple-mobile-web-app-status-bar-style';
      meta.content = 'black-translucent';
      document.head.appendChild(meta);
    }
  }, []);

  return (
    <UIButton onClick={toggleTheme} className="p-[0.35rem]">
      <Sun className="h-4 w-4 rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
      <Moon className="absolute h-4 w-4 rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
      <span className="sr-only">Toggle theme</span>
    </UIButton>
  );
};

export default ThemeToggle;
