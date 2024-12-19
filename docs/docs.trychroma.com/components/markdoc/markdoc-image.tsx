import React from "react";
import Image from "next/image";
import { imageSize } from "image-size";

const MarkdocImage: React.FC<{ src: string; alt: string; title?: string }> = ({
  src,
  alt,
}) => {
  try {
    const { width, height } = imageSize(`public/${src}`);
    return <Image src={src} alt={alt} width={width} height={height} priority />;
  } catch (e) {
    return <div />;
  }
};

export default MarkdocImage;
