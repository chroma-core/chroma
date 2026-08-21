"use client";

import React, { useState } from "react";

const ShadowButton: React.FC<{
  children: React.ReactNode;
  onClick?: () => void;
}> = ({ children, onClick }) => {
  const [isPressed, setIsPressed] = useState(false);

  return (
    <div className="relative">
      <div className="absolute w-full h-full bg-black top-0.5 left-0.5"></div>
      <button
        className={`relative flex items-center justify-center w-full bg-white border-2 border-black p-2 transition-all duration-150 ${
          isPressed
            ? "transform translate-x-0.5 translate-y-0.5 hover:bg-gray-100"
            : "hover:bg-gray-50 hover:-translate-y-0.5 hover:-translate-x-0.5"
        }`}
        onMouseDown={() => setIsPressed(true)}
        onMouseUp={() => setIsPressed(false)}
        onMouseLeave={() => setIsPressed(false)}
        onClick={onClick}
      >
        <div className="flex items-center w-full">{children}</div>
      </button>
    </div>
  );
};

export default ShadowButton;
