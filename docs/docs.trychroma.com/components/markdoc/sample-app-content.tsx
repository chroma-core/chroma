import React from "react";
import CliBlock from "@/components/markdoc/cli-block";
import Image from "next/image";
import { Scale } from "lucide-react";
import TabbedCodeBlock from "@/components/markdoc/tabbed-code-block";
import Tab from "@/components/markdoc/tabs";

const SampleAppContent: React.FC = () => {
  return (
    <div className="flex flex-col w-full">
      <div className="flex items-center justify-between">
        <h1>Chat with your Docs</h1>
      </div>
      <CliBlock>chroma install chat_with_your_docs</CliBlock>
      <div>
        <Image
          src="/chat-with-your-docs.png"
          alt="chat-with-your-docs-preview"
          width={2331}
          height={509}
          priority
          className="rounded-lg border"
        />
      </div>
      <div className="flex justify-between items-stretch">
        <div className="w-[70%]">
          Create an AI-Chat application powered by your own data. This NextJS
          app leverages various retrieval techniques that enable you to query
          your documents with impressive accuracy. The implementation includes
          examples of chunking strategies, query expansion, metadata filtering,
          instruction retrieval approaches, and more. Every interaction with the
          application is logged in a dedicated "telemetry" collection on a
          Chroma DB, giving you visibility into your retrieval pipeline's
          performance. This makes it straightforward to spot anomalies, identify
          knowledge gaps, and continually refine your system based on real usage
          patterns.
        </div>
        <div className="flex flex-col -mt-2">
          <div>
            <p className="font-bold leading-[0px]">Technologies Used</p>
            <p className="leading-[0px]">NextJS</p>
          </div>
          <div>
            <p className="font-bold leading-[0px]">Publisher</p>
            <p className="leading-[0px]">Chroma</p>
          </div>
          <div>
            <p className="font-bold leading-[0px]">Date Published</p>
            <p className="leading-[0px]">03/12/2025</p>
          </div>
          <div className="flex items-center gap-2">
            <Scale className="w-5 h-5" />
            <p className="leading-[0px]">Apache-2.0 license</p>
          </div>
          <div>
            <p className="font-bold leading-[0px]">Languages</p>
            <div className="w-full h-1.5 rounded-full bg-blue-500" />
            <div className="flex items-center gap-2 mt-1">
              <div className="w-2 h-2 rounded-full bg-blue-500" />
              <p className="leading-[0px] text-sm">Typescript (100%)</p>
            </div>
          </div>
        </div>
      </div>
      <div>
        <h3>Getting Started</h3>
        <p>
          Install the Chroma CLI via our Python or NPM package, or using cURL
        </p>
      </div>
    </div>
  );
};

export default SampleAppContent;
