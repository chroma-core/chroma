"use client";

import { codeToHtml, createHighlighter } from 'shiki'
import { CodeChunk } from "@/app/util";
import React from "react";

const supportedLanguages = [
  'javascript',
  'typescript',
  'python',
  'go',
  'java',
  'ruby',
  'php',
  'rust',
]
const theme = 'vitesse-black';
const highlighter = await createHighlighter({
  themes: [theme],
  langs: supportedLanguages,
})

function Code({ codeChunk }: { codeChunk: CodeChunk }) {
  const language = codeChunk.language ?? 'text';
  const highlightedCode = highlighter.codeToHtml(codeChunk.source_code, {
    lang: language,
    theme: theme
  });
  return (
    <div dangerouslySetInnerHTML={{ __html: highlightedCode }} className="overflow-x-scroll"></div>
  )
}

export function SearchResult({ codeChunk }: { codeChunk: CodeChunk }) {

  const output = (
    <div className="mac-style mac-style-hover row-span-3 flex grow flex-col justify-between border border-black bg-black text-white">
      <div className="px-5 pt-3 pb-2 font-mono text-md uppercase">{codeChunk.file_path}</div>
      <hr className="mt-1 mb-3"></hr>
      <div className="mb-4 flex flex-col gap-4 px-5 text-sm max-w-full">
        <div className='w-full'>
          <Code codeChunk={codeChunk} />
        </div>
      </div>
    </div>
  );
  if (codeChunk.url) {
    return (
      <a href={codeChunk.url} target="_blank" rel="noopener noreferrer">
        {output}
      </a>
    )
  } else {
    return output;
  }
}

export function SearchResultSkeleton() {
  return (<div role="status" className="max-w-sm animate-pulse">
    <div className="h-2.5 bg-gray-200 rounded-full dark:bg-gray-700 w-48 mb-4"></div>
    <div className="h-2 bg-gray-200 rounded-full dark:bg-gray-700 max-w-[360px] mb-2.5"></div>
    <div className="h-2 bg-gray-200 rounded-full dark:bg-gray-700 mb-2.5"></div>
    <div className="h-2 bg-gray-200 rounded-full dark:bg-gray-700 max-w-[330px] mb-2.5"></div>
    <div className="h-2 bg-gray-200 rounded-full dark:bg-gray-700 max-w-[300px] mb-2.5"></div>
    <div className="h-2 bg-gray-200 rounded-full dark:bg-gray-700 max-w-[360px]"></div>
    <span className="sr-only">Loading...</span>
  </div>);
}