import { fullTextSearch } from "@/actions";
import { NextRequest, NextResponse } from "next/server";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { query } = body;

    if (!query) {
      return NextResponse.json({ error: 'Search query is required' }, { status: 400 });
    }

    if (query.length === 0) {
      return NextResponse.json({ error: 'Search query must be non-empty' }, { status: 400 });
    }

    return NextResponse.json(await fullTextSearch(query));
  } catch (error) {
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
