"use client"

import * as React from "react"

import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import { DialogTitle } from "@/components/ui/dialog"
import { EnrichedTweetModel } from "@/types"
import { formatDate } from "@/util"
import { useRouter } from "next/navigation"

export default function Search() {
  const [open, setOpen] = React.useState(false)
  const [query, setQuery] = React.useState("")
  const [searchResults, setSearchResults] = React.useState<EnrichedTweetModel[]>([])
  const [isLoading, setIsLoading] = React.useState(false)
  const [error, setError] = React.useState<string | null>(null)

  const abortControllerRef = React.useRef<AbortController | null>(null)

  const router = useRouter()

  const [debouncedQuery, setDebouncedQuery] = React.useState("")

  React.useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(query)
    }, 300)

    return () => clearTimeout(timer)
  }, [query])

  React.useEffect(() => {
    if (query.trim() === "") {
      setSearchResults([])
      setError(null);
    }
  }, [query])

  React.useEffect(() => {
    if (debouncedQuery.trim() === "") {
      return
    }

    const performSearch = async () => {
      // Cancel any existing request
      if (abortControllerRef.current) {
        abortControllerRef.current.abort()
      }

      // Create new AbortController for this request
      const abortController = new AbortController()
      abortControllerRef.current = abortController

      setIsLoading(true)
      setError(null)

      try {
        const response = await fetch('/api/search/fulltext', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            query: debouncedQuery
          }),
          signal: abortController.signal
        });

        if (!response.ok) {
          throw new Error(`Search failed: ${response.statusText}`);
        }

        const results = await response.json();
        setSearchResults(results);
        console.log("set search", results);
      } catch (err) {
        // Don't show error if request was aborted
        if (err instanceof Error && err.name === 'AbortError') {
          return
        }
        setError(err instanceof Error ? err.message : 'Search failed')
        setSearchResults([])
      } finally {
        // Only update loading state if this controller hasn't been aborted
        if (!abortController.signal.aborted) {
          setIsLoading(false)
        }
      }
    }

    performSearch()
  }, [debouncedQuery])

  React.useEffect(() => {
    const down = (e: KeyboardEvent) => {
      if (["j", "k", "p"].includes(e.key) && (e.metaKey || e.ctrlKey)) {
        e.preventDefault()
        setOpen((open) => !open)
      }
    }

    document.addEventListener("keydown", down)
    return () => document.removeEventListener("keydown", down)
  }, [])

  const handleResultClick = (post: EnrichedTweetModel) => {
    setOpen(false)
    router.push(`/post/${post.id}`)
  }

  const truncateText = (text: string, maxLength: number) => {
    if (text.length <= maxLength) return text
    return text.slice(0, maxLength) + "..."
  }

  if (!open) {
    return null;
  }

  const emptyQuery = query.trim() === "";

  let message = <>Start typing to search...</>
  if (!isLoading && debouncedQuery.trim() !== "" && searchResults.length === 0) {
    message = <>No results found.</>
  } else if (isLoading || !emptyQuery) {
    message = <>Loading...</>;
  } else if (error) {
    message = <>Error loading results.</>
  }

  console.log(query, debouncedQuery, searchResults, searchResults.length);

  return (
    <>
      <CommandDialog open={open} onOpenChange={setOpen}>
        <DialogTitle className="sr-only">Search</DialogTitle>
        <CommandInput
          placeholder="Search posts..."
          value={query}
          onValueChange={setQuery}
        />
        <CommandList>
          {(emptyQuery || searchResults.length === 0) && (
            <div className="p-4 text-sm text-muted-foreground">{message}</div>
          )}
          <div>
            {!emptyQuery && searchResults.map((post) => (
              <div
                key={post.id}
                onClick={() => handleResultClick(post)}
                className="flex flex-col items-start gap-1 p-3 cursor-pointer hover:bg-gray-100"
              >
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span>{formatDate(post.date)}</span>
                </div>
                <div className="text-sm">
                  {truncateText(post.body, 150)}
                </div>
              </div>
            ))}
          </div>
        </CommandList>
      </CommandDialog>
    </>
  )
}
