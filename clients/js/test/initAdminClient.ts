import { AdminClient } from "../src/AdminClient";

const PORT = process.env.PORT || "8000";
const URL = "http://localhost:" + PORT;
const adminClient = new AdminClient({ path: URL });

export default adminClient;
