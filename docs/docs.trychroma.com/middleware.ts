import { NextRequest, NextResponse } from "next/server";

const legacyPathsMapping: Record<string, string> = {
  "/getting-started": "/docs/overview/getting-started",
  "/guides": "/docs/run-chroma/ephemeral-client",
  "/guides/embeddings": "/docs/embeddings/embedding-functions",
  "/guides/multimodal": "/docs/embeddings/multimodal",
  "/integrations": "/integrations/chroma-integrations",
  "/integrations/openai": "/integrations/embedding-models/openai",
  "/integrations/cohere": "/integrations/embedding-models/cohere",
  "/integrations/google-gemini": "/integrations/embedding-models/google-gemini",
  "/integrations/hugging-face-server":
    "/integrations/embedding-models/hugging-face-server",
  "/integrations/hugging-face": "/integrations/embedding-models/hugging-face",
  "/integrations/instructor": "/integrations/embedding-models/instructor",
  "/integrations/jinaai": "/integrations/embedding-models/jina-ai",
  "/integrations/ollama": "/integrations/embedding-models/ollama",
  "/integrations/roboflow": "/integrations/embedding-models/roboflow",
  "/integrations/langchain": "/integrations/frameworks/langchain",
  "/integrations/llamaindex": "/integrations/frameworks/llamaindex",
  "/integrations/deepeval": "/integrations/frameworks/deepeval",
  "/integrations/braintrust": "/integrations/frameworks/braintrust",
  "/integrations/haystack": "/integrations/frameworks/haystack",
  "/integrations/openllmetry": "/integrations/frameworks/openllmetry",
  "/integrations/streamlit": "/integrations/frameworks/streamlit",
  "/integrations/openlit": "/integrations/frameworks/openlit",
  "/deployment": "/production/deployment",
  "/deployment/client-server-mode":
    "/production/chroma-server/client-server-mode",
  "/deployment/thin-client": "/production/chroma-server/python-thin-client",
  "/deployment/docker": "/production/containers/docker",
  "/deployment/aws": "/production/cloud-providers/aws",
  "/deployment/azure": "/production/cloud-providers/azure",
  "/deployment/gcp": "/production/cloud-providers/gcp",
  "/deployment/performance": "/production/administration/performance",
  "/deployment/observability": "/production/administration/observability",
  "/deployment/migration": "/updates/migration",
  "/production/administration/migration": "/updates/migration",
  "/deployment/auth": "/production/administration/auth",
  "/telemetry": "/docs/overview/telemetry",
  "/roadmap": "/docs/overview/roadmap",
  "/contributing": "/docs/overview/contributing",
  "/about": "/docs/overview/about",
  "/reference": "/reference/chroma-reference",
  "/reference/py-client": "/reference/python/client",
  "/reference/py-collection": "/reference/python/collection",
  "/reference/js-client": "/reference/js/client",
  "/reference/js-collection": "/reference/js/collection",
  "/reference/cli": "/cli/run",
  "/troubleshooting": "/updates/troubleshooting",
};

export const middleware = (request: NextRequest) => {
  const path = request.nextUrl.pathname;

  if (path in legacyPathsMapping) {
    const currentPath = legacyPathsMapping[path];
    return NextResponse.redirect(new URL(currentPath, request.url));
  }

  return NextResponse.next();
};
