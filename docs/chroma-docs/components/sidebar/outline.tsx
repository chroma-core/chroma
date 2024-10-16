import React from "react";
import { PageMetadata } from "@/lib/content";
import { Playfair_Display } from "next/font/google";
import path from "path";

const playfairDisplay = Playfair_Display({
  subsets: ["latin"],
  display: "swap",
  weight: "400",
  variable: "--font-playfair-display",
});

const Outline: React.FC<{
  title: string;
  pages: PageMetadata[];
  path: string;
}> = ({ title, pages, path }) => {
  return (
    <div className="select-none cursor-pointer">
      <p className={`${playfairDisplay.className} mb-2`}>{title}</p>
      {pages
        .sort((a, b) => a.order - b.order)
        .map((page) => (
          <div
            key={page.title}
            className={`pl-7 py-0.5 border-l border-gray-300 hover:border-gray-900 dark:border-gray-500 dark:hover:border-gray-200 ${path.endsWith(page.id) && "border-gray-900 dark:border-gray-200 font-bold"}`}
          >
            <p className="text-sm">{page.title}</p>
          </div>
        ))}
    </div>
  );
};

export default Outline;
