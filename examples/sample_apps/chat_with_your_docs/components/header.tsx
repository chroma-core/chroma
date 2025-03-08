import React from "react";
import ChromaLogo from "../public/chroma-workmark-color-128.svg";

const Header: React.FC = () => {
  return (
    <div className="flex justify-start items-center w-full px-5 py-3">
      <div>
        <ChromaLogo />
      </div>
    </div>
  );
};

export default Header;
