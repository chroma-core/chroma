import React from "react";

const CenteredContent: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  return <div className="flex items-center justify-center">{children}</div>;
};

export default CenteredContent;
