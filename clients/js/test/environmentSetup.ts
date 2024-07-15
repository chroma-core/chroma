import path from "node:path";
import { GenericContainer, Wait } from "testcontainers";
import { ChromaClient } from "../src/ChromaClient";

const CHROMADB_PORT = 8000;

export async function startChromaContainer() {
  const buildContextDir = path.join(__dirname, "../../..");
  let container: GenericContainer;
  if (process.env.PREBUILT_CHROMADB_IMAGE) {
    container = new GenericContainer(process.env.PREBUILT_CHROMADB_IMAGE);
  } else {
    container = await GenericContainer.fromDockerfile(buildContextDir).build(
      undefined,
      {
        deleteOnExit: false,
      },
    );
  }

  const startedContainer = await container
    .withExposedPorts(CHROMADB_PORT)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(120_000)
    .withEnvironment({
      ANONYMIZED_TELEMETRY: "False",
      ALLOW_RESET: "True",
      IS_PERSISTENT: "True",
    })
    .start();

  return {
    client: new ChromaClient({
      path: `http://${startedContainer.getHost()}:${startedContainer.getMappedPort(
        CHROMADB_PORT,
      )}`,
    }),
    container: startedContainer,
  };
}
