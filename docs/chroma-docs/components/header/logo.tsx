import React from "react";
import ChromaLogo from "../../public/chroma-workmark-color-128.svg";
import OutlineLogo from "../../public/chroma-wordmark-white-128.svg";

const Logo: React.FC = () => {
  const logoClass = "w-28 h-10";

  return (
    <div className="relative">
      <ChromaLogo className={`${logoClass} dark:hidden`} />
      <OutlineLogo className={`${logoClass} hidden dark:inline-block`} />
    </div>
  );
};

export default Logo;
