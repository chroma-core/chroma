import React from "react";
import Sidebar from "@/components/sidebar/sidebar";
import path from "path";
import fs from "fs";
import matter from "gray-matter";

interface LayoutProps {
  children: React.ReactNode;
  params: { slug: string[] };
}

const PageLayout: React.FC<LayoutProps> = ({ children, params }) => {
  const { slug } = params;
  return (
    <div className="flex h-full w-full">
      <Sidebar path={slug} />
      <div className="flex-1 h-full overflow-y-auto">{children}</div>
    </div>
  );
};

export default PageLayout;
