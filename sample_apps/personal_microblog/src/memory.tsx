import { TweetModelBase } from "./types";

export default function remember(query: string, chromaCollection: any): string[] {
    const results = chromaCollection.query({
        queryTexts: [query],
        nResults: 5,
    });
    return results.map((result: any) => result.id);
}
