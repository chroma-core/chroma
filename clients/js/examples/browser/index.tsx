import React from "react";
import ReactDOM from "react-dom/client";
import { App } from "./app";

// Display which package we're using
const packageType = import.meta.env.VITE_PACKAGE || 'default (bundled)';
console.log(`Using ChromaDB package: ${packageType}`);

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
