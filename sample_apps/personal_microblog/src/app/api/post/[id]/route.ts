
import { NextResponse } from "next/server";
import { getPostById } from "@/actions";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;

    if (!id) {
      return NextResponse.json({ error: 'Post ID is required' }, { status: 400 });
    } else if (Array.isArray(id)) {
      return NextResponse.json({ error: 'Post ID must be a single string' }, { status: 400 });
    }

    return NextResponse.json(await getPostById(id));
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
