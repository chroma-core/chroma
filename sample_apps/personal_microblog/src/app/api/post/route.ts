import { getPosts } from "@/actions";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);

  try {
    const cursorParam = searchParams.get('cursor');
    let cursor = cursorParam ? parseInt(cursorParam) : undefined;
    if (cursor != undefined && (cursor < 0 || isNaN(cursor))) {
      cursor = undefined;
    }
    const {posts, cursor: newCursor} = await getPosts(cursor);
    if (newCursor == undefined) {
      throw new Error("newCursor is undefined");
    }
    return NextResponse.json({
      posts,
      cursor: newCursor,
    });
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
