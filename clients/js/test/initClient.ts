import { ChromaClient } from "../src/ChromaClient";

const PORT = process.env.PORT || "8000";
const URL = "http://localhost:" + PORT;
const chroma = new ChromaClient(URL);

export default chroma;
