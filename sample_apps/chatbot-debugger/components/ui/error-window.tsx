import React from "react";
import SysUIContainer from "@/components/ui/sysui-container";
import { TriangleAlert } from "lucide-react";

const ErrorWindowTitle: React.FC = () => {
  return (
    <div className="flex items-center gap-2 px-1">
      <TriangleAlert className="w-4 h-4" />
      <p>ERROR</p>
    </div>
  );
};

const ErrorWindow: React.FC<{ message: string; onClick?: () => void }> = ({
  message,
  onClick,
}) => {
  return (
    <SysUIContainer
      className="w-[350px]"
      title={<ErrorWindowTitle />}
      onClick={onClick}
    >
      <div className="px-7 pt-3 pb-7 bg-red-100 text-sm">{message}</div>
    </SysUIContainer>
  );
};

export default ErrorWindow;
