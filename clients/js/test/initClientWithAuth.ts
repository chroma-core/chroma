import {ChromaClient} from "../src/ChromaClient";

const PORT = process.env.PORT || "8000";
const URL = "http://localhost:" + PORT;
export const chromaBasic = new ChromaClient({path: URL, auth: {provider: "basic", credentials: "admin:admin"}});
export const chromaTokenDefault = new ChromaClient({path: URL, auth: {provider: "token", credentials: "test-token"}});
export const chromaTokenBearer = new ChromaClient({
    path: URL,
    auth: {provider: "token", credentials: "test-token", providerOptions: {headerType: "AUTHORIZATION"}}
});
export const chromaTokenXToken = new ChromaClient({
    path: URL,
    auth: {provider: "token", credentials: "test-token", providerOptions: {headerType: "X_CHROMA_TOKEN"}}
});
