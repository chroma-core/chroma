"use client";

import React from "react";
import Image from "next/image";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";

interface MarkdocImageProps {
  lightSrc: string;
  darkSrc: string;
  alt: string;
  title?: string;
  width?: number;
  height?: number;
}

const MarkdocImage: React.FC<MarkdocImageProps> = ({
  lightSrc,
  darkSrc,
  alt,
  title,
  width = 800,
  height = 400,
}) => {
  const { theme, resolvedTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return (
      <Image
        src={lightSrc}
        alt={alt}
        title={title}
        width={width}
        height={height}
        priority
        className="transition-opacity duration-200"
      />
    );
  }

  const currentTheme = resolvedTheme || theme;
  const imageSrc = currentTheme === "dark" ? darkSrc : lightSrc;

  return (
    <Image
      src={imageSrc}
      alt={alt}
      title={title}
      width={width}
      height={height}
      priority
      className="transition-opacity duration-200"
    />
  );
};

export default MarkdocImage;
