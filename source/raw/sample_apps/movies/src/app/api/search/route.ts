import { queryMovies } from "@/lib/chroma";
import { NextResponse } from "next/server";

export async function POST(req: Request) {
  const { query }: { query: string } = await req.json();

  const movieResults = await queryMovies(query);

  return NextResponse.json({ results: movieResults.results });
}
