import { NextResponse } from "next/server";
import { ChromaClient } from "chromadb";
// @ts-ignore
import { Collection } from "chromadb/src/Collection";

const chromaClient = new ChromaClient({
  path: "https://api.trychroma.com:8000",
  auth: {
    provider: "token",
    credentials: process.env.CHROMA_CLOUD_API_KEY,
    tokenHeaderType: "X_CHROMA_TOKEN",
  },
  tenant: process.env.CHROMA_CLOUD_TENANT,
  database: "docs",
});

const collection: Collection = await chromaClient.getOrCreateCollection({
  name: "docs-content",
});

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const query = searchParams.get("q");

    if (!query) {
      return NextResponse.json(
        { error: "Query parameter is required" },
        { status: 400 },
      );
    }

    let results: {
      distance: number;
      title: string;
      pageTitle: string;
      pageUrl: string;
    }[] = [];

    const queryResults = await collection.query({
      queryTexts: [query],
      include: ["metadatas"],
      where:
        results.length > 0
          ? { pageTitle: { $nin: results.map((r) => r.pageTitle) } }
          : undefined,
    });

    results.push(
      ...queryResults.metadatas[0].map(
        (
          m: {
            pageTitle: string;
            title: string;
            page: string;
            section: string;
            subsection?: string;
          },
          index: number,
        ) => {
          return {
            title: m.title,
            pageTitle: m.pageTitle,
            pageUrl: m.subsection
              ? `/${m.section}/${m.subsection}/${m.page}${m.pageTitle !== m.title ? `#${m.title.replaceAll(" ", "-").replaceAll("_", "-").toLowerCase()}` : ""}`
              : `/${m.section}/${m.page}${m.pageTitle !== m.title ? `#${m.title.replaceAll(" ", "-").replaceAll("_", "-").toLowerCase()}` : ""}`,
          };
        },
      ),
    );

    results = Array.from(
      new Map(results.map((item) => [item.title, item])).values(),
    );

    return NextResponse.json(results);
  } catch (error) {
    console.log(error);
    return NextResponse.json({ error: "Search failed" }, { status: 500 });
  }
}
