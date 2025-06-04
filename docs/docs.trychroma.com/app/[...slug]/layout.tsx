import React from "react";
import Sidebar from "@/components/sidebar/sidebar";

interface LayoutProps {
  children: React.ReactNode;
  params: { slug: string[] };
}

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
