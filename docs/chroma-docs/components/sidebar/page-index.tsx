import React from "react";
import { Playfair_Display } from "next/font/google";
import PageLink from "@/components/sidebar/page-link";

const playfairDisplay = Playfair_Display({
  subsets: ["latin"],
  display: "swap",
  weight: "400",
  variable: "--font-playfair-display",
});

const PageIndex: React.FC<{
  path: string;
  pages: { id: string; name: string }[];
  name?: string;
}> = ({ path, pages, name }) => {
  return (
    <div className="select-none cursor-pointer">
      {name && <p className={`${playfairDisplay.className} mb-2`}>{name}</p>}
      <div className="flex flex-col">
        {pages.map((page) => (
          <PageLink
            key={page.id}
            id={page.id}
            name={page.name}
            path={`${path}/${page.id}`}
          />
        ))}
      </div>
    </div>
  );
};

export default PageIndex;
