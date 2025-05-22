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
          { id: "python-http-client", name: "Python HTTP-Only Client" },
        ],
      },
      {
        id: "collections",
        name: "Collections",
        pages: [
          { id: "create-get-delete", name: "Create, Get, Delete" },
          { id: "configure", name: "Configure" },
          { id: "add-data", name: "Add Data" },
          { id: "update-data", name: "Update Data" },
          { id: "delete-data", name: "Delete Data" },
        ],
      },
      {
        id: "querying-collections",
        name: "Querying Collections",
        pages: [
          { id: "query-and-get", name: "Query And Get" },
          { id: "metadata-filtering", name: "Metadata Filtering" },
          { id: "full-text-search", name: "Full Text Search" },
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
    ],
  },
  {
    id: "cloud",
    name: "Chroma Cloud",
    icon: CloudIcon,
    tag: "",
    pages: [{ id: "getting-started", name: "Getting Started" }],
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
    default: "install",
    icon: SquareTerminalIcon,
    pages: [{ id: "install", name: "Installing the CLI" }],
    subsections: [
      {
        id: "commands",
        name: "Commands",
        pages: [
          { id: "browse", name: "Browse Collections" },
          { id: "copy", name: "Copy Collections" },
          { id: "db", name: "DB Management" },
          { id: "install", name: "Install Sample Apps" },
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
