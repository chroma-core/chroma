import React from "react";
import { Metadata } from "next";
import { capitalize } from "@/lib/utils";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";

export async function generateMetadata({
  params,
}: {
  params: { slug: string };
}): Promise<Metadata> {
  const title = `${capitalize(params.slug)} - Chroma Docs`;
  return {
    title,
  };
}

const Page: React.FC<{ params: { slug: string } }> = ({ params }) => {
  const { slug } = params;
  return <MarkdocRenderer slug={["updates", slug]} />;
};

export default Page;
