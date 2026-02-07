import type { MetadataRoute } from "next";
import fs from "fs";
import matter from "gray-matter";
import path from "path";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import type { AppSection } from "@/lib/content";

const baseUrl = (process.env.NEXT_PUBLIC_SITE_URL ?? "https://docs.trychroma.com").replace(/\/$/, "");
const markdocRoot = path.join(process.cwd(), "markdoc", "content");

interface RouteCandidate {
  segments: string[];
  filePath?: string;
}

const withMarkdownExtension = (segments: string[]) =>
  `${path.join(markdocRoot, ...segments)}.md`;

const getFileLastModified = (filePath?: string) => {
  if (!filePath) {
    return undefined;
  }

  try {
    return fs.statSync(filePath).mtime;
  } catch {
    return undefined;
  }
};

const readGeneratedRoutes = (segments: string[]): RouteCandidate[] => {
  const dirPath = path.join(markdocRoot, ...segments);
  if (!fs.existsSync(dirPath)) {
    return [];
  }

  return fs
    .readdirSync(dirPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".md"))
    .map((entry) => {
      const filePath = path.join(dirPath, entry.name);
      let pageId = entry.name.replace(/\.md$/, "");

      try {
        const { data } = matter(fs.readFileSync(filePath, "utf-8"));
        if (typeof data.id === "string") {
          pageId = data.id;
        }
      } catch {
        // ignore frontmatter issues and fall back to the filename
      }

      return { segments: [...segments, pageId], filePath };
    });
};

const collectSectionRoutes = (
  section: AppSection,
  ancestorSegments: string[] = [],
): RouteCandidate[] => {
  const currentSegments = [...ancestorSegments, section.id];
  const routes: RouteCandidate[] = [];

  if (section.generatePages) {
    routes.push(...readGeneratedRoutes(currentSegments));
  }

  if (section.pages) {
    section.pages.forEach((page) => {
      const segments = [...currentSegments, page.id];
      routes.push({ segments, filePath: withMarkdownExtension(segments) });
    });
  }

  section.subsections?.forEach((subsection) => {
    routes.push(...collectSectionRoutes(subsection, currentSegments));
  });

  return routes;
};

const collectMarkdocRoutes = (): RouteCandidate[] => {
  const routes: RouteCandidate[] = [];

  sidebarConfig.forEach((section) => {
    routes.push(...collectSectionRoutes(section));
  });

  // Include update pages if/when they exist in markdoc/content/updates
  routes.push(...readGeneratedRoutes(["updates"]));

  return routes;
};

const addRoute = (
  routes: Map<string, Date | undefined>,
  pathname: string,
  lastModified?: Date,
) => {
  const normalized = pathname === "/" ? "/" : `/${pathname.replace(/^\/+|\/+$/g, "")}`;

  if (!routes.has(normalized)) {
    routes.set(normalized, lastModified);
    return;
  }

  if (lastModified) {
    const existing = routes.get(normalized);
    if (!existing || lastModified > existing) {
      routes.set(normalized, lastModified);
    }
  }
};

const buildSitemapEntries = (): MetadataRoute.Sitemap => {
  const routes = new Map<string, Date | undefined>();

  addRoute(routes, "/", getFileLastModified(path.join(process.cwd(), "app", "page.tsx")));
  addRoute(
    routes,
    "/cloud",
    getFileLastModified(path.join(process.cwd(), "app", "cloud", "page.tsx")),
  );

  collectMarkdocRoutes().forEach(({ segments, filePath }) => {
    const pathname = `/${segments.join("/")}`;
    addRoute(routes, pathname, getFileLastModified(filePath));
  });

  return Array.from(routes.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([pathname, lastModified]) => ({
      url: `${baseUrl}${pathname}`,
      lastModified,
    }));
};

export default function sitemap(): MetadataRoute.Sitemap {
  return buildSitemapEntries();
}
