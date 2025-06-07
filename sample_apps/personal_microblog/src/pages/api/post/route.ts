import { getPosts } from "@/actions";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);

  try {
    const pageParam = searchParams.get('page');
    let page = pageParam ? parseInt(pageParam) : 0;
    if (page == null || page < 0 || isNaN(page)) {
      page = 0;
    }

    return NextResponse.json(await getPosts(page));
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
