import React from "react";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import { AppSection } from "@/lib/content";

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

const Page: React.FC<{ params: { slug: string[] } }> = ({ params }) => {
  const { slug } = params;
  return <MarkdocRenderer slug={slug} />;
};

export default Page;
