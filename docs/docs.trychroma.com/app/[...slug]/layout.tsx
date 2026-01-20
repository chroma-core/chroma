import React from "react";
import Sidebar from "@/components/sidebar/sidebar";

interface LayoutProps {
  children: React.ReactNode;
  params: Promise<{ slug: string[] }>;
}

async function PageLayout({ children, params }: LayoutProps) {
  const { slug } = await params;
  return (
    <div className="flex h-full w-full">
      <Sidebar path={slug} />
      <div className="flex-1 h-full overflow-y-auto">{children}</div>
    </div>
  );
}

export default PageLayout;
