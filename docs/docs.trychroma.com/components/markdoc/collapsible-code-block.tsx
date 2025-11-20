import React from "react";
import CodeBlock, { CodeBlockProps } from "@/components/markdoc/code-block";

function isCodeBlockElement(
  node: React.ReactNode,
): node is React.ReactElement<CodeBlockProps> {
  if (!React.isValidElement(node)) return false;

  if (node.type === CodeBlock) return true;
  const t = node.type as any;
  return (
    typeof t === "function" &&
    (t.displayName === "CodeBlock" || t.name === "CodeBlock")
  );
}

function injectIntoCodeBlock(
  node: React.ReactNode,
  className: string,
): React.ReactNode {
  return React.Children.map(node, (child) => {
    if (!React.isValidElement(child)) return child;

    if (isCodeBlockElement(child)) {
      const prev = child.props.className ?? "";
      return React.cloneElement<Partial<CodeBlockProps>>(child, {
        className,
      });
    }

    return child;
  });
}

const CollapsibleCodeBlock: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const className = "max-h-64 overflow-y-auto";
  const enhanced = injectIntoCodeBlock(children, className);

  return <div className="bg-black">{enhanced}</div>;
};

export default CollapsibleCodeBlock;
