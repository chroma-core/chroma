'use client';

import SearchBar from '@/components/searchbar';
import { SearchResult, SearchResultSkeleton } from '@/components/search-result';
import SideBar from '@/components/side-bar';
import { useSearchEngineContext } from '../contexts/search-engine-context';
import React from 'react';
import { CodeChunk, waitForConnection } from '../util';
import { useRouter } from 'next/navigation'
import Box from '@/components/chroma/box';

type state = 'searching' | 'result' | 'connecting' | 'error';

function Connecting() {
  return (
    <div className="items-center justify-center h-full">
      <div className="max-w-2xl mx-auto mt-8">
        <Box title={'Currently trying to reconnect'}>
          <div>It looks like the connection to the backend server was lost!</div>
          <div>This might happen if the server crashed due to an error in the code.</div>
          <div>Please relaunch the server.</div>
        </Box>
      </div>
    </div>
  );
}

function Error({ error }: { error: string }) {
  return (
    <div className="items-center justify-center h-full">
      <div className="max-w-2xl mx-auto mt-8">
        <Box title={'Error returned from backend server'}>
          <div>The backend server encountered the following error:</div>
          <div>{error}</div>
        </Box>
      </div>
    </div>
  );
}

function SearchResultsSkeleton() {
  return (
    <div className="space-y-4">
      <SearchResultSkeleton />
      <SearchResultSkeleton />
      <SearchResultSkeleton />
      <SearchResultSkeleton />
    </div>
  );
}

function SearchResults({ results }: { results: CodeChunk[] }) {
  if (!results || results.length === 0) {
    return (
      <div className="max-w-2xl mx-auto mt-8">
        <Box title={'No results found'}>
          <div>
            The backend server successfully returned a list of results for the query, but the list was empty.
          </div>
          <div>
            This might happen because the Chroma collection is empty. Make sure the collection specified in <code>vars.py</code> has documents in it.
          </div>
          <p>You can use the Chroma CLI to check: run <code>chroma run</code> to launch a Chroma server. Then, in a new terminal, use <code>chroma browse [collection name] --local</code></p>
        </Box>
      </div>
    );
  }
  return (
    <div className="space-y-4">
      {
        results.map((result, index) => {
          return <SearchResult key={index} codeChunk={result} />;
        })
      }
    </div>
  );
}

export default function SearchPage() {
  const context = useSearchEngineContext();
  const {
    hostUrl,
    query,
    setQuery,
  } = context!;

  if (!hostUrl || !query) {
    const router = useRouter();
    React.useEffect(() => {
      router.push('/');
    }, []);
  }

  const [state, setState] = React.useState<state>('searching');
  const [results, setResults] = React.useState<CodeChunk[]>([]);

  const onSearch = (query: string) => {
    setQuery(query);
    setState('searching');
  };

  React.useEffect(() => {
    if (state === 'connecting') {
      const fetchHealth = async () => {
        await waitForConnection(hostUrl + '/api/health', state !== 'connecting');
        setState('searching');
      };
      fetchHealth();
    } else if (state === 'result') {
      return;
    } else if (state === 'searching' && hostUrl && query) {
      fetch(hostUrl + '/api/query?q=' + query)
        .then((res) => res.json())
        .then((data) => {
          if (data['error']) {
            setState('error');
            return;
          }
          setResults(data['result']);
          setState('result');
        })
        .catch((error) => {
          setState('connecting');
        });
    }
  }, [state]);

  return (
    <div className="flex bg-neutral-900 max-w-screen">
      <SideBar />

      <main className="flex-1 p-6 max-w-full">
        <div className="overflow-y-scroll max-w-full">
          <div className="mb-6 width-full">
            <SearchBar initialValue={query ?? ''} onSearch={onSearch} />
          </div>
          <div className="space-y-4">
            {
              state === 'searching' ? <SearchResultsSkeleton />
                : state === 'result' ? <SearchResults results={results} />
                  : state === 'connecting' ? <Connecting />
                    : <Error error={''} />
            }
          </div>
        </div>
      </main>
    </div>
  );
}