"use client";

import React, { JSX } from 'react';
import Editor from 'react-simple-code-editor';

import styles from './searchbar.module.css';

function highlightConstant(queryFragment: RegExpExecArray): JSX.Element {
  const styles = {
    color: '#0969da',
    backgroundColor: '#ddf4ff',
    borderRadius: '.1875rem',
  };
  return (<span style={styles}>{queryFragment[0]}</span>);
}

function highlightFilterValue(queryFragment: RegExpExecArray): JSX.Element {
  const styles = {
    color: '#0969da',
    backgroundColor: '#ddf4ff',
    borderRadius: '.1875rem',
  };
  return (<span style={styles}>{queryFragment[0]}</span>);
}

const HIGHLIGHT_FUNCTIONS: Record<string, [RegExp, (queryFragment: RegExpExecArray) => JSX.Element]> = {
  'constant': [/\/(.*?)\/(g)?/, highlightConstant],
  'filter': [/(^|\s)(language|filename|extension|in):([^\s]+)/, highlightFilterValue],
}
function highlight(query: string): JSX.Element {
  let result: (string | JSX.Element)[] = [query];
  let keyCounter = 0;

  Object.entries(HIGHLIGHT_FUNCTIONS).forEach(([_, [regex, highlightFn]]) => {
    result = result.flatMap(segment => {
      if (typeof segment !== 'string') return [segment];

      const parts: (string | JSX.Element)[] = [];
      let lastIndex = 0;

      for (const match of segment.matchAll(new RegExp(regex, 'g'))) {
        if (match.index === undefined) continue;
        if (match.index > lastIndex) {
          parts.push(segment.slice(lastIndex, match.index));
        }
        const highlightedElement = React.cloneElement(
          highlightFn(match),
          { key: `highlight-${keyCounter++}` }
        );
        parts.push(highlightedElement);
        lastIndex = match.index + match[0].length;
      }

      if (lastIndex < segment.length) {
        parts.push(segment.slice(lastIndex));
      }
      return parts;
    });
  });

  return (
    <div>
      {result.map((element, index) =>
        typeof element === 'string'
          ? <span key={`text-${index}`}>{element}</span>
          : element
      )}
    </div>
  );
}

export default function SearchBar({ initialValue, onSearch }: { initialValue?: string, onSearch: (query: string) => void }) {
  const [query, setQuery] = React.useState<string>(initialValue ?? '');

  const handleKeyDown: React.EventHandler<React.KeyboardEvent<Element>> = (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      if (query.trim().length > 0) {
        onSearch(query);
      }
    }
  };

  return (
    <div className="w-full min-2-lg mx-auto bg-white border-2">
      <Editor
        value={query}
        onValueChange={newQuery => setQuery(newQuery)}
        highlight={query => highlight(query)}
        padding={10}
        style={{
          fontFamily: '"Fira code", "Fira Mono", monospace',
          fontSize: '120%',
          height: '2.5em',
          textWrap: 'nowrap',
          overflowY: 'hidden',
          wordWrap: "normal",
          whiteSpace: "nowrap"
        }}
        placeholder={'Search codebase...'}
        onKeyDown={handleKeyDown}
        textareaClassName={'search-text-area'}
      />
    </div>
  );
}
