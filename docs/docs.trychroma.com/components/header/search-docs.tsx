"use client";

import React, { useEffect, useState } from "react";
import { Dialog, DialogContent, DialogTrigger } from "../ui/dialog";
import UIButton from "@/components/ui/ui-button";
import { Cross2Icon, MagnifyingGlassIcon } from "@radix-ui/react-icons";
import * as DialogPrimitive from "@radix-ui/react-dialog";
import _ from "lodash";
import { Input } from "@/components/ui/input";
import ChromaIcon from "../../public/chroma-icon.svg";
import { AlertTriangleIcon, ArrowRight, Loader } from "lucide-react";
import Link from "next/link";

const SearchDocs: React.FC = () => {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<
    { title: string; pageTitle: string; pageUrl: string }[]
  >([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const debouncedSearch = _.debounce(async (searchQuery: string) => {
    if (!searchQuery.trim()) {
      setResults([]);
      return;
    }

    try {
      setIsLoading(true);
      setError(null);

      const response = await fetch(
        `/api/search?q=${encodeURIComponent(searchQuery)}`,
      );

      if (!response.ok) {
        throw new Error("Search failed");
      }

      const data = await response.json();
      setResults(data);
      console.log(data);
    } catch (err) {
      setError("Failed to perform search");
      setResults([]);
    } finally {
      setIsLoading(false);
    }
  }, 300);

  useEffect(() => {
    debouncedSearch(query);

    return () => {
      debouncedSearch.cancel();
    };
  }, [query]);

  return (
    <Dialog
      onOpenChange={(open) => {
        if (!open) {
          setQuery("");
        }
      }}
    >
      <DialogTrigger asChild>
        <UIButton className="lex items-center gap-2 p-[0.35rem] px-3 text-xs">
          <MagnifyingGlassIcon className="w-4 h-4" />
          <p>Search...</p>
        </UIButton>
      </DialogTrigger>
      <DialogContent className="h-96 flex flex-col gap-0 sm:rounded-none p-0">
        <div className="relative py-2 px-[3px] h-fit border-b-[1px] border-black dark:border-gray-300">
          <div className="flex flex-col gap-0.5">
            {[...Array(7)].map((_, index) => (
              <div
                key={index}
                className="w-full h-[1px] bg-black dark:bg-gray-300"
              />
            ))}
            <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 px-2 py-1 bg-white dark:bg-gray-950 font-mono">
              Search Docs
            </div>
            <div className="absolute right-4 top-[6px] px-1 bg-white dark:bg-gray-950">
              <DialogPrimitive.Close className="flex items-center justify-center bg-white dark:bg-gray-950 border-[1px] border-black disabled:pointer-events-none ">
                <Cross2Icon className="h-5 w-5" />
                <span className="sr-only">Close</span>
              </DialogPrimitive.Close>
            </div>
          </div>
        </div>
        <div className="relativ px-4 my-4">
          <Input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search..."
            className="w-full p-2 border border-black rounded-none"
          />
        </div>
        <div className="flex-grow overflow-y-scroll px-4">
          {isLoading && (
            <div className="flex items-center justify-center w-full h-full">
              <Loader className="w-5 h-5 animate-spin" />
            </div>
          )}
          {error && (
            <div className="flex flex-col gap-2 items-center justify-center w-full h-full">
              <AlertTriangleIcon className="w-5 h-5 text-red-500" />
              <p className="text-xs">
                Failed to fetch results. Try again later
              </p>
            </div>
          )}
          {!isLoading && !error && (
            <div className="flex flex-col gap-2 pb-10">
              {results.map((result, index) => (
                <Link
                  key={`result-${index}`}
                  href={result.pageUrl}
                  onClick={() => setQuery("")}
                >
                  <DialogPrimitive.Close className="flex justify-between items-center w-full text-start p-3 border-[1.5px] h-16 hover:border-black dark:hover:border-blue-300 cursor-pointer">
                    <div className="flex flex-col gap-1">
                      <p className="text-sm font-semibold">
                        {result.title || result.pageTitle}
                      </p>
                      {result.title && result.title !== result.pageTitle && (
                        <p className="text-xs">{result.pageTitle}</p>
                      )}
                    </div>
                    <ArrowRight className="w-5 h-5" />
                  </DialogPrimitive.Close>
                </Link>
              ))}
            </div>
          )}
        </div>
        <div className="flex justify-end py-2 px-4 border-t border-black">
          <Link
            href="https://airtable.com/appG6DhLoDUnTawwh/shrOAiDUtS2ILy5vZ"
            target="_blank"
            rel="noopener noreferrer"
          >
            <div className="flex items-center gap-2">
              <ChromaIcon className="h-7 w-7" />
              <p className="text-xs font-semibold">Powered by Chroma Cloud</p>
            </div>
          </Link>
        </div>
      </DialogContent>
    </Dialog>
  );
};

export default SearchDocs;
