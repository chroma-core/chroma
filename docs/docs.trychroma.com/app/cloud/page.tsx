import React from "react";
import Sidebar from "@/components/sidebar/sidebar";

const CloudPage: React.FC = () => {
  return (
    <div className="flex h-full flex-grow overflow-hidden pb-5">
      <Sidebar path={["cloud"]} />
      <div className="flex items-center justify-center flex-grow overflow-y-auto prose">
        <h1>Coming Soon</h1>
      </div>
    </div>
  );
};

export default CloudPage;
