import React from "react";
import { imageSize } from "image-size";
import Image from "next/image";

const StyledImage: React.FC<{
  src: string;
  alt: string;
  className: string;
}> = ({ src, alt, className }) => {
  try {
    const { width, height } = imageSize(`public/${src}`);
    return (
      <div className="w-full">
        <Image
          src={src}
          alt={alt}
          width={width}
          height={height}
          className={className}
          priority
        />
      </div>
    );
  } catch (e) {
    return <div />;
  }
};

export default StyledImage;
