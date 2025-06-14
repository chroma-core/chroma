import { TweetModelBase } from "@/types";
import { chromaClient, chromaCollection } from "@/clients";
import {
  chromaGetResultsToPostModels,
} from "@/util";
import { NextRequest, NextResponse } from "next/server";

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const { id } = params;

    if (!id) {
      return NextResponse.json({ error: 'Post ID is required' }, { status: 400 });
    }

    const replies = await chromaCollection.get({
      where: { threadParentId: id },
      include: ["documents", "metadatas"],
    });

    const result = chromaGetResultsToPostModels(replies);
    return NextResponse.json(result);
  } catch (error) {
    console.error('API Error:', error);
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 });
  }
}
