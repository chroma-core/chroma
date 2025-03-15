import React from "react";

const CenteredContent: React.FC<{
  children: React.ReactNode;
  className?: string;
}> = ({ children, className }) => {
  return (
    <div
      className={`flex items-center justify-center rounded-lg ring-1 ${className}`}
    >
      {children}
    </div>
  );
};

export default CenteredContent;
