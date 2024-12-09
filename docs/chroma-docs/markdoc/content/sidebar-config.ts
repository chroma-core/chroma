import {
  BlocksIcon,
  BookText,
  GraduationCap,
  type LucideIcon,
  RocketIcon,
  SquareTerminalIcon,
  WrenchIcon,
} from "lucide-react";

export interface AppSection {
  id: string;
  name: string;
  default?: string;
  icon?: LucideIcon;
  pages?: { id: string; name: string }[];
  generatePages?: boolean;
  subsections?: AppSection[];
  comingSoon?: boolean;
}

const sidebarConfig: AppSection[] = [
  {
    id: "docs",
    name: "Docs",
    default: "/overview/introduction",
    icon: BookText,
    subsections: [
      {
        id: "overview",
        name: "Overview",
        pages: [
          {
            id: "introduction",
            name: "Introduction",
          },
          {
            id: "getting-started",
            name: "Getting Started",
          },
          {
            id: "roadmap",
            name: "Roadmap",
          },
          {
            id: "contributing",
            name: "Contributing",
          },
          {
            id: "About",
            name: "About",
          },
        ],
      },
      {
        id: "guides",
        name: "Guides",
        pages: [
          { id: "usage-guide", name: "Usage Guide" },
          { id: "embeddings-guide", name: "Embedding" },
          { id: "multimodal", name: "Multimodal" },
        ],
      },
    ],
  },
  {
    id: "production",
    name: "Production",
    default: "deployment",
    icon: RocketIcon,
    pages: [{ id: "deployment", name: "Deployment" }],
    subsections: [
      {
        id: "chroma-server",
        name: "Chroma Server",
        pages: [
          { id: "client-server-mode", name: "Client Server Mode" },
          { id: "python-thin-client", name: "Python Thin Client" },
        ],
      },
      {
        id: "containers",
        name: "Containers",
        pages: [{ id: "docker", name: "Docker" }],
      },
      {
        id: "cloud-providers",
        name: "Cloud Providers",
        pages: [
          { id: "aws", name: "AWS" },
          { id: "azure", name: "Azure" },
          { id: "gcp", name: "GCP" },
        ],
      },
      {
        id: "administration",
        name: "Administration",
        pages: [
          { id: "performance", name: "Performance" },
          { id: "observability", name: "Observability" },
          { id: "migration", name: "Migration" },
          { id: "auth", name: "Auth" },
        ],
      },
    ],
  },
  {
    id: "integrations",
    name: "Integrations",
    default: "chroma-integrations",
    icon: BlocksIcon,
    pages: [{ id: "chroma-integrations", name: "Chroma Integrations" }],
    subsections: [
      {
        id: "embedding-models",
        name: "Embedding Models",
        generatePages: true,
      },
      {
        id: "frameworks",
        name: "Frameworks",
        generatePages: true,
      },
    ],
  },
  {
    id: "cli",
    name: "CLI",
    icon: SquareTerminalIcon,
    pages: [{ id: "vacuum", name: "Vacuum" }],
  },
  {
    id: "reference",
    name: "Reference",
    icon: WrenchIcon,
    default: "chroma-reference",
    pages: [{ id: "chroma-reference", name: "Chroma Reference" }],
    subsections: [
      {
        id: "python",
        name: "Python",
        pages: [
          { id: "client", name: "Client" },
          { id: "collection", name: "Collection" },
        ],
      },
      {
        id: "js",
        name: "JavaScript",
        pages: [
          { id: "client", name: "Client" },
          { id: "collection", name: "Collection" },
        ],
      },
    ],
  },
  {
    id: "learn",
    name: "Learn",
    icon: GraduationCap,
    comingSoon: true,
  },
];

export default sidebarConfig;
