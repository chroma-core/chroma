import React from "react";

const Banner: React.FC<{ type: string; children: React.ReactNode }> = ({
  type,
  children,
}) => {
  const styles: Record<string, string> = {
    note: "bg-yellow-500",
    tip: "bg-blue-500",
    warn: "bg-red-500 ",
  };

  return (
    <div className="my-7">
      <div className="relative border-[1px] px-7 border-gray-900 bg-white dark:bg-black dark:border-gray-600">
        <div
          className={`absolute top-1.5 left-1.5 w-full h-full -z-10 ${styles[type]}`}
        />
        {children}
      </div>
    </div>
  );
};

export default Banner;
