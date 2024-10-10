import {
  BlocksIcon,
  BookText,
  GraduationCap,
  type LucideIcon,
  SquareTerminalIcon,
  Wrench,
} from "lucide-react";
import path from "path";
import fs from "fs";
import Markdoc from "@markdoc/markdoc";

export interface AppSection {
  id: string;
  name: string;
  target: string;
  icon: LucideIcon;
  subSections: string[];
}

export interface PageMetadata {
  id: string;
  title: string;
  section: string;
  order: number;
}

const layoutConfig: AppSection[] = [
  {
    id: "docs",
    name: "Docs",
    target: "/docs",
    icon: BookText,
    subSections: ["Overview"],
  },
  {
    id: "integrations",
    name: "Integrations",
    target: "/integrations",
    icon: BlocksIcon,
    subSections: [],
  },
  {
    id: "cli",
    name: "CLI",
    target: "/cli",
    icon: SquareTerminalIcon,
    subSections: [],
  },
  {
    id: "reference",
    name: "Reference",
    target: "/reference",
    icon: Wrench,
    subSections: [],
  },
  {
    id: "learn",
    name: "Learn",
    target: "/learn",
    icon: GraduationCap,
    subSections: [],
  },
];

const getSubsection = (dirPath: string, name: string): PageMetadata => {
  const filePath = path.join(dirPath, name);
  const source = fs.readFileSync(filePath, "utf-8");

  const ast = Markdoc.parse(source);
  const frontmatter = ast.attributes.frontmatter
    ? JSON.parse(ast.attributes.frontmatter)
    : {};

  return {
    id: frontmatter.id,
    title: frontmatter.title,
    section: frontmatter.section,
    order: frontmatter.order,
  };
};

export const getSectionDirectory = (section: string) => {
  const dirPath = path.join(process.cwd(), "markdoc", "content", section);
  return fs
    .readdirSync(dirPath)
    .map((child) => getSubsection(dirPath, child))
    .reduce(
      (acc, page) => {
        if (!acc[page.section]) {
          acc[page.section] = [];
        }
        acc[page.section].push(page);
        return acc;
      },
      {} as Record<string, PageMetadata[]>,
    );
};

export default layoutConfig;
