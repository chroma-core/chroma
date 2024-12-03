import React from "react";
import {InfoCircledIcon} from "@radix-ui/react-icons";

const Banner:React.FC<{type: string; children: React.ReactNode}> = ({type, children}) => {
    const styles: Record<string, string> = {
        note: "border-yellow-500 bg-yellow-50 dark:bg-yellow-100",
        tip: "border-blue-500 bg-blue-50 dark:bg-blue-100",
        warn: "border-red-500 bg-red-50 dark:bg-red-100",
    }
    return <div className={`relative border-4 dark:text-gray-800 rounded-sm border-double text-sm px-4 mt-2 ${styles[type]}`}>
        <div className="flex gap-2 items-start">
            <InfoCircledIcon className="w-4 h-4 flex-shrink-0 mt-5" />
            <div className="">{children}</div>
        </div>
    </div>
}

export default Banner