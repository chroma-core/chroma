import React from "react";
import Sidebar from "@/components/sidebar/sidebar";

const PageLayout: React.FC<{
  children: React.ReactNode;
  params: { slug: string[] };
}> = ({ children, params }) => {
  const { slug } = params;

  return (
    <div className="flex flex-grow overflow-hidden">
      <Sidebar path={slug} />
      <div className="flex-grow overflow-y-auto">{children}</div>
    </div>
  );
};

export default PageLayout;
