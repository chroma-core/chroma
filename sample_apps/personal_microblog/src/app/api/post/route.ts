import { getPosts } from "@/actions";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);

  try {
    const pageParam = searchParams.get('page');
    let page = pageParam ? parseInt(pageParam) : undefined;
    if (page != undefined && (page < 0 || isNaN(page))) {
      page = undefined;
    }
    const {posts, cursor} = await getPosts(page);
    return NextResponse.json({
      posts,
      cursor,
    });
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
