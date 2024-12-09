import React from "react";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";

// TODO: Add page metadata for SEO

const Page: React.FC<{ params: { slug: string[] } }> = ({ params }) => {
  const { slug } = params;
  return <MarkdocRenderer slug={slug} />;
};

export default Page;
