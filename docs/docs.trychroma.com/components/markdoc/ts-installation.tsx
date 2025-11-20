import React from "react";
import TabbedUseCaseCodeBlock from "@/components/markdoc/tabbed-use-case-code-block";
import Tab from "@/components/markdoc/tabs";
import CodeBlock from "@/components/markdoc/code-block";

const TypescriptInstallation: React.FC<{ packages: string }> = ({
  packages,
}) => {
  return (
    <TabbedUseCaseCodeBlock language="Terminal">
      <Tab label="npm">
        <CodeBlock
          content={`npm install ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
      <Tab label="pnpm">
        <CodeBlock
          content={`pnpm add ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
      <Tab label="yarn">
        <CodeBlock
          content={`yarn add ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
      <Tab label="bun">
        <CodeBlock
          content={`bun add ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
    </TabbedUseCaseCodeBlock>
  );
};

export default TypescriptInstallation;
