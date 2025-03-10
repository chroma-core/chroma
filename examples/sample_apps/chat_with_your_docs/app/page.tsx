import ChatContainer from "@/components/chat/chat-container";
import InputBox from "@/components/chat/input-box";

export default function Home() {
  return (
    <div className="flex p-5 items-center justify-center w-full h-full">
      <ChatContainer>
        <div className="flex flex-col w-full h-full">
          <div className="flex-grow"></div>
          <div className="flex items-center justify-center p-5">
            <InputBox />
          </div>
        </div>
      </ChatContainer>
    </div>
  );
}
