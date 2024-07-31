import path from "node:path";
import { GenericContainer, Wait } from "testcontainers";

const CHROMADB_PORT = 8000;

const BUILD_CONTEXT_DIR = path.join(__dirname, "../../..");

const BASIC_AUTH_FILE = path.join(__dirname, "test.htpasswd");

export async function startChromaContainer({
  authType,
}: {
  authType?: "basic" | "token" | "xtoken";
} = {}) {
  let container: GenericContainer;
  if (process.env.PREBUILT_CHROMADB_IMAGE) {
    container = new GenericContainer(process.env.PREBUILT_CHROMADB_IMAGE);
  } else {
    container = await GenericContainer.fromDockerfile(BUILD_CONTEXT_DIR).build(
      undefined,
      {
        deleteOnExit: false,
      },
    );
    container = container.withCopyFilesToContainer([
      {
        source: BASIC_AUTH_FILE,
        target: "/chromadb/test.htpasswd",
      },
    ]);
  }

  const env: Record<string, string> = {
    ANONYMIZED_TELEMETRY: "False",
    ALLOW_RESET: "True",
    IS_PERSISTENT: "True",
  };

  switch (authType) {
    case "basic":
      env.CHROMA_SERVER_AUTHN_PROVIDER =
        "chromadb.auth.basic_authn.BasicAuthenticationServerProvider";
      env.CHROMA_SERVER_AUTHN_CREDENTIALS_FILE = "/chromadb/test.htpasswd";
      break;
    case "token":
      env.CHROMA_SERVER_AUTHN_CREDENTIALS = "test-token";
      env.CHROMA_SERVER_AUTHN_PROVIDER =
        "chromadb.auth.token_authn.TokenAuthenticationServerProvider";
      break;
    case "xtoken":
      env.CHROMA_AUTH_TOKEN_TRANSPORT_HEADER = "X-Chroma-Token";
      env.CHROMA_SERVER_AUTHN_CREDENTIALS = "test-token";
      env.CHROMA_SERVER_AUTHN_PROVIDER =
        "chromadb.auth.token_authn.TokenAuthenticationServerProvider";
      break;
  }

  const startedContainer = await container
    // uncomment to see container logs
    // .withLogConsumer((stream) => {
    //   stream.on("data", (line) => console.log(line));
    //   stream.on("err", (line) => console.error(line));
    //   stream.on("end", () => console.log("Stream closed"));
    // })
    .withExposedPorts(CHROMADB_PORT)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(120_000)
    .withEnvironment(env)
    .start();

  const chromaUrl = `http://${startedContainer.getHost()}:${startedContainer.getMappedPort(
    CHROMADB_PORT,
  )}`;

  return {
    url: chromaUrl,
    host: startedContainer.getHost(),
    port: startedContainer.getMappedPort(CHROMADB_PORT),
    container: startedContainer,
  };
}
