import React from "react";

const generateId = (content: React.ReactNode): string => {
  if (typeof content === "string") {
    return content
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .replace(/\s+/g, "-")
      .trim();
  }
  return "";
};

const Heading: React.FC<{
  level: number;
  children: React.ReactNode;
  id?: string;
}> = ({ level, children, id }) => {
  const HeadingTag: React.ElementType = `h${level}` as React.ElementType;
  const headingId = id || generateId(children);

  return (
    <HeadingTag id={headingId} className={`group`}>
      {children}
      {headingId && level === 2 && (
        <a
          href={`#${headingId}`}
          className="ml-2 opacity-0 group-hover:opacity-100 transition-opacity"
          aria-label={`Link to ${headingId}`}
        >
          #
        </a>
      )}
    </HeadingTag>
  );
};

export default Heading;
