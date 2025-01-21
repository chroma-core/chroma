import React from "react";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import { AppSection } from "@/lib/content";
import { Metadata } from "next";
import { capitalize } from "@/lib/utils";

export const generateStaticParams = async () => {
  const slugs: string[][] = [];

  const traverseSection = (section: AppSection, path: string[] = []) => {
    if (section.pages) {
      section.pages.forEach((page) => {
        slugs.push([...path, page.id]);
      });
    }

    if (section.subsections) {
      section.subsections.forEach((subsection) => {
        traverseSection(subsection, [...path, subsection.id]);
      });
    }
  };

  sidebarConfig.forEach((section) => {
    traverseSection(section, [section.id]);
  });

  return slugs.map((slug) => ({
    slug,
  }));
};

export async function generateMetadata({
  params,
}: {
  params: { slug: string[] };
}): Promise<Metadata> {
  const title = `${params.slug[params.slug.length - 1]
    .split("-")
    .map((s) => capitalize(s))
    .join(" ")} - Chroma Docs`;
  return {
    title,
  };
}

const Page: React.FC<{ params: { slug: string[] } }> = ({ params }) => {
  const { slug } = params;
  return <MarkdocRenderer slug={slug} />;
};

export default Page;
