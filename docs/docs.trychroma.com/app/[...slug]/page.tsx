import React from "react";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import { AppSection } from "@/lib/content";
import { Metadata } from "next";
import { capitalize } from "@/lib/utils";
import fs from "fs";
import path from "path";

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
  // Try to read a human-friendly title from the Markdoc file
  const filePath = `${path.join(
    process.cwd(),
    "markdoc",
    "content",
    ...params.slug,
  )}.md`;

  let pageTitle: string | undefined;
  try {
    if (fs.existsSync(filePath)) {
      const source = fs.readFileSync(filePath, "utf-8");
      // Prefer frontmatter `name:` if present
      const fmMatch = source.match(/^---\n([\s\S]*?)\n---\n/);
      if (fmMatch) {
        const nameMatch = fmMatch[1].match(/(?:^|\n)name:\s*['"]?(.+?)['"]?(?:\n|$)/);
        if (nameMatch?.[1]) {
          pageTitle = nameMatch[1].trim();
        }
      }
      // Fallback to first H1 heading
      if (!pageTitle) {
        const h1Match = source.match(/^\s*#\s+(.+)\s*$/m);
        if (h1Match?.[1]) {
          pageTitle = h1Match[1].trim();
        }
      }
    }
  } catch {
    // ignore and fallback
  }

  const title = `${
    pageTitle ||
    params.slug[params.slug.length - 1]
      .split("-")
      .map((s) => capitalize(s))
      .join(" ")
  } - Chroma Docs`;
  return {
    title,
  };
}

const Page: React.FC<{ params: { slug: string[] } }> = ({ params }) => {
  const { slug } = params;
  return <MarkdocRenderer slug={slug} />;
};

export default Page;
