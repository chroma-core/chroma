import React from "react";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import { AppSection } from "@/lib/content";
import { Metadata } from "next";
import { capitalize } from "@/lib/utils";
import fs from "fs";
import path from "path";
import matter from "gray-matter";

// Ensure all pages are statically generated at build time
export const dynamicParams = false;

// Helper to read pages from filesystem for generatePages subsections
const getGeneratedPages = (slugPath: string[]): { id: string; name: string }[] => {
  const dirPath = path.join(process.cwd(), "markdoc", "content", ...slugPath);
  try {
    const files = fs.readdirSync(dirPath);
    const pages: { id: string; name: string }[] = [];

    for (const file of files) {
      if (file.endsWith(".md")) {
        const filePath = path.join(dirPath, file);
        const content = fs.readFileSync(filePath, "utf-8");
        const { data } = matter(content);
        if (data.id && data.name) {
          pages.push({ id: data.id, name: data.name });
        }
      }
    }
    return pages;
  } catch {
    return [];
  }
};

export const generateStaticParams = async () => {
  const slugs: string[][] = [];

  const traverseSection = (section: AppSection, currentPath: string[] = []) => {
    if (section.pages) {
      section.pages.forEach((page) => {
        slugs.push([...currentPath, page.id]);
      });
    }

    if (section.subsections) {
      section.subsections.forEach((subsection) => {
        // Handle subsections with generatePages: true
        if (subsection.generatePages) {
          const generatedPages = getGeneratedPages([...currentPath, subsection.id]);
          generatedPages.forEach((page) => {
            slugs.push([...currentPath, subsection.id, page.id]);
          });
        } else {
          traverseSection(subsection, [...currentPath, subsection.id]);
        }
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
