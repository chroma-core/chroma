import React from "react";
import TabbedUseCaseCodeBlock from "@/components/markdoc/tabbed-use-case-code-block";
import Tab from "@/components/markdoc/tabs";
import CodeBlock from "@/components/markdoc/code-block";

const PythonInstallation: React.FC<{ packages: string }> = ({ packages }) => {
  return (
    <TabbedUseCaseCodeBlock language="Terminal">
      <Tab label="pip">
        <CodeBlock
          content={`pip install ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
      <Tab label="poetry">
        <CodeBlock
          content={`poetry add ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
      <Tab label="uv">
        <CodeBlock
          content={`uv pip install ${packages}`}
          language="terminal"
          showHeader={false}
        />
      </Tab>
    </TabbedUseCaseCodeBlock>
  );
};

export default PythonInstallation;
