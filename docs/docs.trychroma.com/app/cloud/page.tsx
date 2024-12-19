import React from "react";
import Sidebar from "@/components/sidebar/sidebar";

const CloudPage: React.FC = () => {
  return (
    <div className="flex h-full flex-grow overflow-hidden pb-5">
      <Sidebar path={["cloud"]} />
      <div className="flex-grow overflow-y-auto">
        <iframe
          className="airtable-embed"
          src="https://airtable.com/embed/appG6DhLoDUnTawwh/shrOAiDUtS2ILy5vZ"
          width="100%"
          height="100%"
        ></iframe>
      </div>
    </div>
  );
};

export default CloudPage;
