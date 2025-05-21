"use client"

import { useState } from "react";


function Lines() {
  return (
    <div className="flex flex-col justify-between">
      <div className="h-[2px] bg-black"></div>
      <div className="h-[2px] bg-black"></div>
      <div className="h-[2px] bg-black"></div>
      <div className="h-[2px] bg-black"></div>
      <div className="h-[2px] bg-black"></div>
      <div className="h-[2px] bg-black"></div>
    </div>
  );
}

interface AboutModalProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function AboutModal({ isOpen, onClose }: AboutModalProps) {
  const [position, setPosition] = useState({ x: 400, y: 400 });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });

  const handleMouseDown = (e: React.MouseEvent) => {
    setIsDragging(true);
    setDragStart({
      x: e.clientX - position.x,
      y: e.clientY - position.y
    });
  };

  const handleMouseMove = (e: React.MouseEvent) => {
    if (isDragging) {
      setPosition({
        x: e.clientX - dragStart.x,
        y: e.clientY - dragStart.y
      });
    }
  };

  const handleMouseUp = () => {
    setIsDragging(false);
  };

  const handleClose = () => {
    onClose();
    setPosition({ x: window.innerWidth / 2 - 200, y: window.innerHeight / 2 - 150 });
  };

  if (!isOpen) return null;

  return (
    <div
      className="absolute p-4 max-w-md max-h-full"
      style={{
        left: position.x,
        top: position.y,
        transform: 'translate(-50%, -50%)'
      }}
      onClick={(e) => e.stopPropagation()}
    >
      <div className="relative bg-white text-black border-2 border-black shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
        <div className="p-2 border-b-2 border-black">
          <div
            className="absolute bg-white w-[1.7em] h-[1.7em] left-[1em] p-[.1em] m-[-.1em] cursor-pointer"
            onClick={handleClose}>
            <div className="border-2 w-full h-full">

            </div>
          </div>
          <div className="grid grid-cols-[1fr_.1fr_1fr] justify-between w-full cursor-move"
            onMouseDown={handleMouseDown}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onMouseLeave={handleMouseUp}>
            <Lines />
            <div className="flex flex-row items-center px-4 text-md select-none">
              <span>Chroma</span>
            </div>
            <Lines />
          </div>
        </div>

        {/* Modal content */}
        <div className="p-6 space-y-4">
          <p className="text-base text-center">
            Code Search Sample App
          </p>
          <div className="text-sm/4.5 space-y-3 text-gray-700">
            <p>
              A demonstration of Chroma's capabilities for semantic code search.
            </p>
            <p>
              We can leave a message here explaining how to use the app, the features Chroma has that makes
              it good for code search, etc.
            </p>
            <p>
              I think it would be classy if we also left a message saying we appreciate our users.
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}