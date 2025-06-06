"use client"

import * as React from "react"

import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
  CommandShortcut,
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
  const router = useRouter()

  const [debouncedQuery, setDebouncedQuery] = React.useState("")

  React.useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(query)
    }, 300)

    return () => clearTimeout(timer)
  }, [query])

  // Perform search when debounced query changes
  React.useEffect(() => {
    if (debouncedQuery.trim() === "") {
      setSearchResults([])
      setError(null)
      return
    }

    const performSearch = async () => {
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
          })
        })

        if (!response.ok) {
          throw new Error(`Search failed: ${response.statusText}`)
        }

        const results = await response.json()
        console.log(results)
        setSearchResults(results)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Search failed')
        setSearchResults([])
      } finally {
        setIsLoading(false)
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
          <CommandEmpty>No results found.</CommandEmpty>
          <CommandGroup>
            {searchResults.map((post) => {
              const output = <CommandItem
                key={post.id}
                value={post.body}
                onSelect={() => handleResultClick(post)}
                className="flex flex-col items-start gap-1 p-3"
              >
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <span>{formatDate(post.date)}</span>
                </div>
                <div className="text-sm">
                  {truncateText(post.body, 150)}
                </div>
              </CommandItem>
              console.log(output);
              return output;
            })}
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </>
  )
}
