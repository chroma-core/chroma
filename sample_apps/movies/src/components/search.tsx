"use client";

import { useState } from "react";
import {
  InputGroup,
  InputGroupInput,
  InputGroupAddon,
} from "@/components/ui/input-group";
import { SearchIcon, Clapperboard } from "lucide-react";
import { SearchResultRow } from "chromadb";

export default function Search() {
  const [results, setResults] = useState<SearchResultRow[] | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const onSearch = async (query: string) => {
    // TODO: use react-query probably

    try {
      setIsLoading(true);
      const response = await fetch("/api/search", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await response.json();
      setResults(data.results);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      onSearch(e.currentTarget.value);
    }
  };

  return (
    <div className="grid flex-1 grid-rows-[auto_1fr] gap-4 overflow-hidden px-2 pt-4">
      <div>
        <InputGroup className="mx-auto max-w-2xl">
          <InputGroupInput
            placeholder="Search..."
            onKeyDown={handleKeyDown}
            disabled={isLoading}
          />
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
        </InputGroup>
      </div>

      <div className="space-y-3 overflow-y-auto px-1 pb-4">
        {isLoading && (
          <div className="text-muted-foreground grid h-full place-content-center text-sm">
            Searching...
          </div>
        )}

        {!isLoading && results === null && (
          <div className="text-muted-foreground grid h-full place-content-center text-sm">
            <span>Try searching for a movie!</span>
          </div>
        )}

        {!isLoading && results && results.length === 0 && (
          <div className="text-muted-foreground grid h-full place-content-center text-sm">
            <span>No results found.</span>
          </div>
        )}

        {!isLoading &&
          results &&
          results.map((result) => (
            <ResultCard key={result.id} result={result} />
          ))}
      </div>
    </div>
  );
}

function ResultCard({ result }: { result: SearchResultRow }) {
  return (
    <div className="mx-auto flex max-w-2xl items-start justify-between gap-4 rounded-lg border p-4">
      <div className="flex flex-1 items-start gap-3">
        <div className="bg-muted shrink-0 rounded-md p-2">
          <Clapperboard className="text-muted-foreground h-4 w-4" />
        </div>

        <div className="flex-1">
          <span className="text-muted-foreground truncate font-mono text-xs">
            {result.id}
          </span>

          {result.document && (
            <p className="text-foreground line-clamp-3 text-sm">
              {result.document}
            </p>
          )}

          {result.metadata && Object.keys(result.metadata).length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1.5">
              {Object.entries(result.metadata)
                .filter(([key]) => key !== "bm25_sparse_vector")
                .map(([key, value]) => (
                  <span
                    key={key}
                    className="bg-muted text-muted-foreground inline-flex items-center rounded-full px-2 py-0.5 text-xs"
                  >
                    <span className="font-medium">{key}:</span>
                    <span className="ml-1 max-w-[150px] truncate">
                      {String(value)}
                    </span>
                  </span>
                ))}
            </div>
          )}
        </div>
      </div>

      {result.score && (
        <div className="shrink-0 text-right">
          <div className="text-foreground text-sm font-semibold">
            {result.score}
          </div>
          <div className="text-muted-foreground text-xs">score</div>
        </div>
      )}
    </div>
  );
}
