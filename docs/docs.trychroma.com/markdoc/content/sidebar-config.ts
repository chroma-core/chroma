import {
  BlocksIcon,
  BookText,
  GraduationCap,
  RocketIcon,
  SquareTerminalIcon,
  WrenchIcon,
} from "lucide-react";
import { AppSection } from "@/lib/content";
import CloudIcon from "@/components/sidebar/cloud-icon";

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
            id: "architecture",
            name: "Architecture",
          },
          {
            id: "data-model",
            name: "Data Model",
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
            id: "telemetry",
            name: "Telemetry",
          },
          {
            id: "migration",
            name: "Migration",
          },
          {
            id: "troubleshooting",
            name: "Troubleshooting",
          },
          {
            id: "about",
            name: "About",
          },
        ],
      },
      {
        id: "run-chroma",
        name: "Run Chroma",
        pages: [
          { id: "ephemeral-client", name: "Ephemeral Client" },
          { id: "persistent-client", name: "Persistent Client" },
          { id: "client-server", name: "Client-Server Mode" },
          { id: "cloud-client", name: "Cloud Client" },
        ],
      },
      {
        id: "collections",
        name: "Collections",
        pages: [
          { id: "manage-collections", name: "Manage Collections" },
          { id: "add-data", name: "Add Data" },
          { id: "update-data", name: "Update Data" },
          { id: "delete-data", name: "Delete Data" },
          { id: "configure", name: "Configure" },
        ],
      },
      {
        id: "querying-collections",
        name: "Querying Collections",
        pages: [
          { id: "query-and-get", name: "Query And Get" },
          { id: "metadata-filtering", name: "Metadata Filtering" },
          { id: "full-text-search", name: "Full Text Search and Regex" },
        ],
      },
      {
        id: "embeddings",
        name: "Embeddings",
        pages: [
          { id: "embedding-functions", name: "Embedding Functions" },
          { id: "multimodal", name: "Multimodal" },
        ],
      },
      {
        id: "cli",
        name: "CLI",
        pages: [
          { id: "install", name: "Installing the CLI" },
          { id: "browse", name: "Browse Collections" },
          { id: "copy", name: "Copy Collections" },
          { id: "db", name: "DB Management" },
          { id: "sample-apps", name: "Install Sample Apps" },
          { id: "login", name: "Login" },
          { id: "profile", name: "Profile Management" },
          { id: "run", name: "Run a Chroma Server" },
          { id: "update", name: "Update the CLI" },
          { id: "vacuum", name: "Vacuum" },
        ],
      },
    ],
  },
  {
    id: "cloud",
    name: "Chroma Cloud",
    icon: CloudIcon,
    tag: "",
    pages: [
      { id: "getting-started", name: "Getting Started" },
      { id: "pricing", name: "Pricing" },
      { id: "quotas-limits", name: "Quotas & Limits" },
    ],
    subsections: [
      {
        id: "features",
        name: "Features",
        pages: [
          { id: "collection-forking", name: "Collection Forking" },
        ],
      },
      {
        id: "schema",
        name: "Schema",
        pages: [
          { id: "overview", name: "Overview" },
          { id: "schema-basics", name: "Schema Basics" },
          { id: "sparse-vector-search", name: "Sparse Vector Search Setup" },
          { id: "index-reference", name: "Index Configuration Reference" },
        ],
      },
      {
        id: "search-api",
        name: "Search API",
        pages: [
          { id: "overview", name: "Overview" },
          { id: "search-basics", name: "Search Basics" },
          { id: "filtering", name: "Filtering with Where" },
          { id: "ranking", name: "Ranking and Scoring" },
          { id: "hybrid-search", name: "Hybrid Search with RRF" },
          { id: "pagination-selection", name: "Pagination & Selection" },
          { id: "batch-operations", name: "Batch Operations" },
          { id: "examples", name: "Examples & Patterns" },
          { id: "migration", name: "Migration Guide" },
        ],
      },
      {
        id: "sync",
        name: "Sync",
        pages: [
          { id: "overview", name: "Overview" },
          { id: "github", name: "GitHub" },
          { id: "web", name: "Web" },
        ],
      },
      {
        id: "package-search",
        name: "Package Search",
        pages: [
          { id: "mcp", name: "MCP" },
          { id: "registry", name: "Registry" },
        ],
      },
    ],
  },
  {
    id: "guides",
    name: "Guides",
    icon: GraduationCap,
    default: "/build/building-with-ai",
    subsections: [
      {
        id: "build",
        name: "Build",
        pages: [
          { id: "building-with-ai", name: "Building With AI" },
          { id: "intro-to-retrieval", name: "Introduction to Retrieval" },
          // { id: "chunking", name: "Chunking" },
          // { id: "embeddings", name: "Embeddings" },
          // { id: "organizing-collections", name: "Organizing Collections" },
        ],
      },
      // { id: "develop", name: "Develop", generatePages: true },
      {
        id: "deploy",
        name: "Deploy",
        pages: [
          { id: "client-server-mode", name: "Client Server Mode" },
          { id: "python-thin-client", name: "Python Thin Client" },
          { id: "performance", name: "Performance" },
          { id: "observability", name: "Observability" },
          { id: "docker", name: "Docker" },
          { id: "aws", name: "AWS" },
          { id: "azure", name: "Azure" },
          { id: "gcp", name: "GCP" },
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
        name: "JavaScript/Typescript",
        pages: [
          { id: "client", name: "Client" },
          { id: "collection", name: "Collection" },
        ],
      },
    ],
  },
];

export default sidebarConfig;
