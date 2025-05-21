"use client";

import SearchBar from "@/components/searchbar";
import Image from "next/image";
import React from "react";
import { useRouter } from 'next/navigation'

import { useSearchEngineContext } from "./contexts/search-engine-context";
import Box from "@/components/chroma/box";
import { waitForConnection } from "./util";

function Button(props: { text: string, onClick?: any }) {
  return (
    <button onClick={props.onClick} data-slot="button" className="cursor-pointer gap-2 whitespace-nowrap text-sm font-medium transition-[color,box-shadow] disabled:pointer-events-none disabled:opacity-50 [&amp;_svg]:pointer-events-none [&amp;_svg:not([className*='size-'])]:size-4 [&amp;_svg]:shrink-0 ring-ring/10 dark:ring-ring/20 dark:outline-ring/40 outline-ring/50 focus-visible:ring-4 focus-visible:outline-1 aria-invalid:focus-visible:ring-0 bg-primary hover:bg-primary/90 px-4 has-[>svg]:px-3 flex items-center justify-center border-x-[0.9px] border-y shadow-sm outline-hidden h-full rounded py-[0.2rem] text-[#27201C] bg-linear-to-b from-[#FFFFFF] to-[#f9f9f9] border-[#171716]/40 hover:bg-linear-to-b hover:from-gray-100 hover:to-gray-100">{props.text}</button>
  );
}

function Onboard(props: { setState: any, setUrl: any }) {

  return (<div className="grid h-full grid-cols-1 md:grid-cols-[3.5fr_2fr] bg-white">
    <div className="flex flex-col justify-between">
      <div>
        <div className="px-5 pt-3 pb-2 font-mono text-md uppercase">Build My Own Code Search Engine</div>
        <hr className="mt-1 mb-3"></hr>
        <div className="mb-4 px-5 text-sm">Follow this tutorial to learn how to build your own search engine specialized for code.</div>
      </div>
      <div className={"flex flex-col md:flex-row px-5 pb-5 gap-4"}>
        <a target="_blank" rel="noopener noreferrer" href="https://docs.trychroma.com/docs/overview/getting-started">
          <Button text="Open tutorial" />
        </a>
        <Button text="Connect to my local instance" onClick={() => {
          props.setUrl('http://127.0.0.1:3001');
          props.setState('connecting');
        }} />
      </div>
    </div>
    <div className="flex flex-col justify-between bg-[#cfcfcf]">
      <div>
        <div className="px-5 pt-3 pb-2 font-mono text-md uppercase">Use Demo</div>
        <hr className="mt-1 mb-3"></hr>
        <div className="mb-4 px-5 text-sm">Explore the open source Chroma codebase.</div>
      </div>
      <div className="px-5 pb-5">
        <Button text="Start searching" onClick={() => {
          props.setUrl('https://');
          props.setState('connecting');
        }} />
      </div>
    </div>
  </div>
  );
}

function Connecting(props: { setState: any }) {
  return (
    <Box title={'Currently trying to connect'}>
      <div>
        Please run an instance by using the provided script in the tutorial.
      </div>
      <div className="flex flex-row gap-4"><a href={""}><Button text={"Open tutorial"} /></a><a href={""}><Button text={"How to run service"} /></a></div>
    </Box>
  );
}

export default function Home() {
  const context = useSearchEngineContext();
  const {
    hostUrl,
    setHostUrl,
    query,
    setQuery,
  } = context!;

  type state = 'onboarding' | 'connecting' | 'connected';
  const [state, setState] = React.useState<state>('onboarding');

  React.useEffect(() => {
    const fetchHealth = async () => {
      if (state !== 'connecting') {
        return;
      }
      await waitForConnection(hostUrl + '/api/health', state !== 'connecting');
      setState('connected');
    };

    fetchHealth();
  }, [state]);

  const router = useRouter()

  function onSearch(query: string) {
    setQuery(query);
    router.push('/search')
  }

  return (
    <div className="flex-grow-1 grid grid-rows-[1fr_1fr] items-center justify-items-center p-8 pb-20 gap-16 sm:p-20 bg-[url('/background2.png')] bg-cover bg-center bg-no-repeat">
      <Image
        src="/chroma-logo.png"
        width={300}
        height={300}
        alt="Chroma"
        className="invert mix-blend-difference select-none"
      />
      <main className="flex flex-col gap-[32px] self-start w-2xs sm:w-md md:w-2xl">
        {state === 'onboarding' ? (
          <Onboard setState={setState} setUrl={setHostUrl} />
        ) : state === 'connecting' ? (
          <Connecting setState={setState} />
        ) : (
          <SearchBar onSearch={onSearch} />
        )}
      </main>
    </div>
  );
}
