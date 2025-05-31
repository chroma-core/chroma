'use client'

import React, { useState, useCallback } from "react";
import sidebarConfig from "@/markdoc/content/sidebar-config";
import MenuItem from "../sidebar/menu-item";
import { useParams } from "next/navigation";

const HeaderNav: React.FC = () => {
  const params = useParams();
  const [menuOpen, setMenuOpen] = useState(false);
  
  // get current path from url using nextjs router
  const currentSection = sidebarConfig.find((section) =>
    params?.slug && Array.isArray(params.slug) && params.slug.join("").startsWith(section.id),
  );

  const toggleMenu = useCallback(() => {
    setMenuOpen(prev => !prev);
  }, []);

  return (
    <div className="flex flex-col border-b-[1px] dark:border-gray-700">
      {/* Mobile View */}
      <div className="flex justify-between items-center px-4 py-2 md:hidden">
        {/* Current section */}
        {currentSection && (
          <MenuItem
            key={currentSection.id}
            section={currentSection}
            active={true}
          />
        )}
        
        {/* Hamburger menu button */}
        <button 
          type="button"
          onClick={toggleMenu}
          className="flex flex-col justify-center items-center p-2"
          aria-label="Toggle menu"
        >
          <span className={`block w-6 h-0.5 bg-gray-800 dark:bg-gray-200 transition-transform duration-300 ${menuOpen ? 'rotate-45 translate-y-2' : ''}`}/>
          <span className={`block w-6 h-0.5 bg-gray-800 dark:bg-gray-200 mt-1.5 transition-opacity duration-300 ${menuOpen ? 'opacity-0' : ''}`}/>
          <span className={`block w-6 h-0.5 bg-gray-800 dark:bg-gray-200 mt-1.5 transition-transform duration-300 ${menuOpen ? '-rotate-45 -translate-y-2' : ''}`}/>
        </button>
      </div>
      
      {/* Mobile menu dropdown */}
      <div className={`md:hidden px-5 transition-all duration-300 ${menuOpen ? 'max-h-96 overflow-y-auto' : 'max-h-0 overflow-hidden'}`}>
        {sidebarConfig.map((section) => (
          currentSection?.id !== section.id && (
            <MenuItem
              key={section.id}
              section={section}
              active={false}
              onClick={() => setMenuOpen(false)}
            />
          )
        ))}
      </div>
      
      {/* Desktop View */}
      <div className="hidden md:flex flex-row flex-shrink-0 px-5">
        {sidebarConfig.map((section) => (
          <MenuItem
            key={section.id}
            section={section}
            active={currentSection?.id === section.id}
          />
        ))}
      </div>
    </div>
  );
};

export default HeaderNav;