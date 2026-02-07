import React from "react";
import { Metadata } from "next";
import { capitalize } from "@/lib/utils";
import fs from "fs";
import path from "path";
import MarkdocRenderer from "@/components/markdoc/markdoc-renderer";

export async function generateMetadata({
  params,
}: {
  params: { slug: string };
}): Promise<Metadata> {
  // Try to read a human-friendly title from the Markdoc file
  const filePath = `${path.join(
    process.cwd(),
    "markdoc",
    "content",
    "updates",
    params.slug,
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

  const title = `${pageTitle || capitalize(params.slug)} - Chroma Docs`;
  return {
    title,
  };
}

const Page: React.FC<{ params: { slug: string } }> = ({ params }) => {
  const { slug } = params;
  return <MarkdocRenderer slug={["updates", slug]} />;
};

export default Page;
