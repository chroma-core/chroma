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
    verbose = false,
  }: {
    model?: string;
    verbose?: boolean;
  } = { model: "chroma/all-minilm-l6-v2-f32" },
) {
  let container: GenericContainer;
  if (process.env.PREBUILT_CHROMADB_IMAGE) {
    container = new GenericContainer(process.env.PREBUILT_CHROMADB_IMAGE);
  } else {
    container = new GenericContainer("ollama/ollama:latest");
  }

  const env: Record<string, string> = {};

  container = container
    .withExposedPorts(OLLAMA_PORT)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(120_000)
    .withEnvironment(env);

  if (verbose) {
    container = container.withLogConsumer((stream) => {
      stream.on("data", (line) => console.log(line));
      stream.on("err", (line) => console.error(line));
      stream.on("end", () => console.log("Stream closed"));
    });
  }

  const startedContainer = await container.start();

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

  console.log("5");

  await startedContainer.exec(["ollama", "pull", "nomic-embed-text"]);

  console.log("6");

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
