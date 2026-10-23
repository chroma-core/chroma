import { GenericContainer, Wait } from "testcontainers";
const OLLAMA_PORT = 11434;

// checking model name to prevent command injection
function isValidModel(model: string): boolean {
  const regex =
    /^[a-zA-Z0-9\-]+((:[a-zA-Z0-9\-]+)?|(\/[a-zA-Z0-9\-]+)?(:[a-zA-Z0-9\-]+)?)$/; // matches: <model> or <model>:<version> or <user>/<model> or <user>/<model>:<version>
  return regex.test(model);
}

export async function startOllamaContainer(
  {
    model = "chroma/all-minilm-l6-v2-f32",
  }: {
    model?: string;
  } = { model: "chroma/all-minilm-l6-v2-f32" },
) {
  let container: GenericContainer;
  if (process.env.PREBUILT_CHROMADB_IMAGE) {
    container = new GenericContainer(process.env.PREBUILT_CHROMADB_IMAGE);
  } else {
    container = new GenericContainer("ollama/ollama:latest");
  }

  const env: Record<string, string> = {};

  const startedContainer = await container
    // uncomment to see container logs
    // .withLogConsumer((stream) => {
    //   stream.on("data", (line) => console.log(line));
    //   stream.on("err", (line) => console.error(line));
    //   stream.on("end", () => console.log("Stream closed"));
    // })
    .withExposedPorts(OLLAMA_PORT)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(120_000)
    .withEnvironment(env)
    .start();

  const ollamaUrl = `http://${startedContainer.getHost()}:${startedContainer.getMappedPort(
    OLLAMA_PORT,
  )}`;
  if (!model) {
    throw new Error("Model name is required");
  }
  if (model && !isValidModel(model)) {
    throw new Error("Invalid model name");
  }
  await startedContainer.exec(["ollama", "pull", model]);
  await startedContainer.exec(["ollama", "pull", "nomic-embed-text"]);
  await startedContainer.exec([
    "ollama",
    "pull",
    "chroma/all-minilm-l6-v2-f32",
  ]);
  return {
    ollamaUrl: ollamaUrl,
    host: startedContainer.getHost(),
    port: startedContainer.getMappedPort(OLLAMA_PORT),
    ollamaContainer: startedContainer,
  };
}
