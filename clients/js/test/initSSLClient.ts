import { ChromaClient } from "../src/ChromaClient";
const PORT = process.env.PORT || "8443";
const URL = "https://127.0.0.1:" + PORT;

import * as https from 'https';
const fs = require('fs');

// Create a custom agent with SSL certificate verification disabled
const agent = new https.Agent({
    cert: fs.readFileSync("../../certs/servercert.pem", "utf8"),
    rejectUnauthorized: false
});

// @ts-ignore
const chroma = new ChromaClient({ path: URL, fetchOptions: { agent} });

export default chroma;
