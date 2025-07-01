import React from "react";
import Sidebar from "@/components/sidebar/sidebar";
import path from "path";
import fs from "fs";
import matter from "gray-matter";

interface LayoutProps {
  children: React.ReactNode;
  params: { slug: string[] };
}


const generatePages = (slug: string[]): { id: string; name: string }[] => {
  const dirPath = path.join(process.cwd(), "markdoc", "content", ...slug);
  const files = fs.readdirSync(dirPath);

  const pages = [];

  for (const file of files) {
    if (file.endsWith(".md")) {
      const filePath = path.join(dirPath, file);
      const content = fs.readFileSync(filePath, "utf-8");

      const { data } = matter(content);
      if (data.id && data.name) {
        pages.push({ id: data.id, name: data.name });
      }
    }
  }

  return pages;
};

const PageLayout: React.FC<LayoutProps> = ({ children, params }) => {
  const { slug } = params;
  return (
    <div className="flex h-full w-full">
      <div className="shrink-0 h-full overflow-y-auto relative">
        <Sidebar path={slug} />
      </div>
      <div className="flex-1 h-full overflow-y-auto">{children}</div>
    </div>
  );
};

export default PageLayout;
