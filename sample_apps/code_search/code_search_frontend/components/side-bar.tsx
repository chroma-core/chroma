"use client";

import React from 'react';
import { useSearchEngineContext } from '@/app/contexts/search-engine-context';
import Box from './chroma/box';

interface BackendState {
  collection_name: string;
  embedding_function: string;
}

function State() {
  const context = useSearchEngineContext();
  const {
    hostUrl
  } = context!;
  const [connected, setConnected] = React.useState<boolean>(false);
  const [state, setState] = React.useState<BackendState | null>(null);

  React.useEffect(() => {
    const id = setInterval(() => {
      fetch(hostUrl + '/api/state')
        .then((res) => res.json())
        .then((data) => {
          if (data['error']) {
            setConnected(false);
            return;
          }
          setState(data['result']);
          setConnected(true);
        })
        .catch((error) => {
          setConnected(false);
        });
    }, 3000);
    return () => clearInterval(id);
  }, [state, connected])

  let output = null;
  if (!connected) {
    output = (
      <div>Lost connection to backend...</div>
    )
  } else {
    output = <pre>{JSON.stringify(state, null, 2)}</pre>;
  }
  return (<Box title={"Server Status"} className=' text-black font-size-sm mt-2 bg-[#9E9D9E] overflow-x-scroll'>
    {state == null ? <div>Not connected</div> :
      <div className='flex flex-col gap-2'>
        <div className="flex items-center gap-2">
          <div>Connected</div>
          <div className="w-2 h-2 rounded-full bg-green-700"></div>
        </div>
        <hr className='text-gray-600'></hr>
        <div>
          <div>Collection Name:</div>
          <div>{state['collection_name']}</div>
        </div>
        <hr className='text-gray-600'></hr>
        <div>
          <div>Embedding Function:</div>
          <div>{state['embedding_function']}</div>
        </div>
      </div>
    }

  </Box>)
}

export default function SideBar() {
  return (
    <aside className="w-md">
      <div className="border-r border-neutral-700 text-white p-4 flex flex-col gap-4">
      <div>
        <State />
      </div>
      <div>
        <Box title={"Filters"} className='bg-[#9E9D9E] text-black'>
          ''
        </Box>

      </div>
      </div>
    </aside>
  )
}