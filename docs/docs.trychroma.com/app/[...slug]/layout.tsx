import React from "react";
import Sidebar from "@/components/sidebar/sidebar";
import UpdatesNavbar from "@/components/header/updates-navbar";

interface LayoutProps {
  children: React.ReactNode;
  params: { slug: string[] };
}

const PageLayout: React.FC<LayoutProps> = ({ children, params }) => {
  const { slug } = params;

  return (
    <div className={`flex flex-grow overflow-hidden `}>
      <Sidebar path={slug} />
      <div className="flex-grow overflow-y-auto">{children}</div>
    </div>
  );
};

export default PageLayout;
