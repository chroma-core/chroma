import React from "react";
import CliBlock from "@/components/markdoc/cli-block";
import Image from "next/image";
import { GithubIcon, Scale, Triangle } from "lucide-react";
import TabbedCodeBlock from "@/components/markdoc/tabbed-code-block";
import Tab from "@/components/markdoc/tabs";
import TabbedUseCaseCodeBlock from "@/components/markdoc/tabbed-use-case-code-block";
import CodeBlock from "@/components/markdoc/code-block";
import Link from "next/link";

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
        <TabbedUseCaseCodeBlock language="Terminal">
          <Tab label="Python">
            <CodeBlock
              content="pip install chromadb"
              language="python"
              showHeader={true}
              hideTicks
            />
          </Tab>
          <Tab label="Javascript">
            <CodeBlock
              content="npm install chromadb"
              language="python"
              showHeader={true}
              hideTicks
            />
          </Tab>
          <Tab label="cURL">
            <CodeBlock
              content="curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash"
              language="python"
              showHeader={true}
              hideTicks
            />
          </Tab>
        </TabbedUseCaseCodeBlock>
      </div>
      <p>Use the Chroma CLI to install the app and get its data.</p>
      <CodeBlock
        content="chroma install chat_with_your_docs"
        language="Terminal"
        showHeader={true}
        hideTicks
      />
      <p>Run the app locally!</p>
      <CodeBlock
        content="cd chat_with_your_docs && npm run dev"
        language="Terminal"
        showHeader={true}
        hideTicks
      />
      <h3>Code</h3>
      <Link
        href="https://github.com/chroma-core/chroma/tree/itai/demo-cli/examples/sample_apps/chat_with_your_docs"
        target="_blank"
        rel="noopener noreferrer"
      >
        <div className="flex items-center justify-between p-3 rounded-md border border-black">
          <div className="flex items-center gap-4">
            <Image
              src="/nextjs-icon.svg"
              alt="nextjs-icon"
              width={800}
              height={800}
              priority
              className="w-5 h-5 p-0 m-0"
            />
            <p className="leading-[0px]">NextJS app</p>
          </div>
          <GithubIcon className="w-5 h-5 p-0 m-0" />
        </div>
      </Link>
      <Link
        href="https://colab.research.google.com/drive/1Y-U6eGLKkrDyrnj8tfzTos4tIHyo_nhc?usp=sharing"
        target="_blank"
        rel="noopener noreferrer"
      >
        <div className="flex items-center justify-between p-3 rounded-md border border-black mt-2">
          <div className="flex items-center gap-4">
            <Image
              src="/jupyter_logo.svg.png"
              alt="jupyter-icon"
              width={800}
              height={800}
              priority
              className="w-5 h-5 p-0 m-0 grayscale"
            />
            <p className="leading-[0px]">
              Jupyter Notebook: Generate a golden dataset
            </p>
          </div>
          <Image
            src="/colab-icon.png"
            alt="colab-icon"
            width={225}
            height={225}
            priority
            className="w-7 h-7 p-0 m-0 grayscale"
          />
        </div>
      </Link>
    </div>
  );
};

export default SampleAppContent;
