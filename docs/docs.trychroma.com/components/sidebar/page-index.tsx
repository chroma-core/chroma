import React, { Suspense } from "react";
import { Playfair_Display } from "next/font/google";
import PageLink from "@/components/sidebar/page-link";

const playfairDisplay = Playfair_Display({
  subsets: ["latin"],
  display: "swap",
  weight: "500",
  variable: "--font-playfair-display",
});

const PageIndex: React.FC<{
  path: string;
  pages: { id: string; name: string }[];
  name?: string;
}> = ({ path, pages, name }) => {
  return (
    <div className="select-none cursor-pointer">
      {name && (
        <p
          className={`${playfairDisplay.className} mb-2 tracking-wide cursor-default`}
        >
          {name}
        </p>
      )}
      <div className="flex flex-col">
        {pages.map((page) => (
          <Suspense key={page.id}>
            <PageLink
              id={page.id}
              name={page.name}
              path={`${path}/${page.id}`}
              sectionPage={name !== undefined}
            />
          </Suspense>
        ))}
      </div>
    </div>
  );
};

export default PageIndex;
