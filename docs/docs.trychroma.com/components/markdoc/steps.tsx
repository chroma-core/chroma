import React from "react";

const Steps: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  return (
    <div className="relative">
      <div className="pointer-events-none absolute left-[1rem] top-0 bottom-0 w-px bg-black dark:bg-gray-300 -z-20" />

      {Array.isArray(children) &&
        children.map((child, index) => (
          <div
            key={index}
            className="grid grid-cols-[2rem,minmax(0,1fr)] gap-4 items-baseline"
          >
            <div
              className="relative mx-auto flex h-6 w-6 items-center justify-center
                        border border-black dark:border-gray-100 bg-white dark:bg-gray-900 font-mono text-xs leading-none"
            >
              <div className="absolute h-6 w-6 top-[0.5px] left-[0.5px] -z-10 bg-black dark:bg-gray-200" />
              {index + 1}
            </div>

            <div>{child}</div>
          </div>
        ))}
    </div>
  );
};

export const Step: React.FC<{ children: React.ReactNode; title?: string }> = ({
  children,
  title,
}) => {
  return (
    <div className="flex flex-col gap-1">
      {title && <p className="text-lg font-bold">{title}</p>}
      <div>{children}</div>
    </div>
  );
};

export default Steps;
