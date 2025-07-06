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
  basePath: string;
  pages: { id: string; name: string }[];
  name?: string;
  index: number;
}> = ({ basePath, pages, name, index }) => {
  const className = index === 0 ? "" : "border-t-2 pt-6 dark:border-t-gray-700";

  return (
    <div className="select-none cursor-default">
      {name && (
        <div className={className}>
        <p
          className={`font-semibold text-black dark:text-white mb-2 cursor-default text-sm pl-2`}
        >
          {name}
        </p>
        </div>
      )}
      <div className="flex flex-col">
        {pages.map((page) => (
          <Suspense key={page.id}>
            <PageLink
              id={page.id}
              name={page.name}
              slug={`${basePath}/${page.id}`}
              sectionPage={name !== undefined}
            />
          </Suspense>
        ))}
      </div>
    </div>
  );
};

export default PageIndex;
