"use client"

import { useState } from "react";
import AboutModal from "./about-modal";

export default function Footer() {
    const [modalVisible, setModalVisible] = useState(false);
    return (
        <footer className="flex gap-[24px] overflow-hidden bg-black p-5 text-white shrink-0">
            <div className="w-full flex justify-between gap-2 flex-row">
                <div className="text-sm md:text-md">
                    <div className="font-mono">© Chroma 2025</div>
                </div>
                <div className="flex gap-2">
                    <button onClick={() => setModalVisible(true)} className="text-neutral-400 cursor-pointer" type="button">
                        About
                    </button>
                    <span className="text-neutral-400">•</span>
                    <a className="text-neutral-400" href="https://github.com/chroma-core/chroma">View Source</a>
                    <AboutModal isOpen={modalVisible} onClose={() => setModalVisible(false)} />
                </div>
            </div>
        </footer>
    );
}