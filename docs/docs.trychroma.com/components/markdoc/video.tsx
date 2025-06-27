import React from "react";

const Video: React.FC<{ link: string; title: string }> = ({ link, title }) => {
  return (
    <div className="aspect-video">
      <iframe
        src={link}
        title={title}
        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
        allowFullScreen
        className="w-full h-full"
      />
    </div>
  );
};

export default Video;
