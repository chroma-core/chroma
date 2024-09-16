import path from "node:path";
import { exec } from "node:child_process";
import { GenericContainer, Wait } from "testcontainers";
import bcrypt from "bcrypt";

const CHROMADB_PORT = 8000;

const BUILD_CONTEXT_DIR = path.join(__dirname, "../../..");

/** See [this page](https://httpd.apache.org/docs/2.4/misc/password_encryptions.html) for more information about the format of this file. */
const BASIC_AUTH_PASSWORD_FILE_CONTENTS = `admin:${bcrypt.hashSync(
  "admin",
  1,
)}`;

// Manually build image--as of September 2024, testcontainers does not support BuildKit
async function buildDockerImage(
  dockerfilePath: string,
  imageName: string,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const absoluteDockerfilePath = path.resolve(dockerfilePath);

    const buildContextDir = path.dirname(absoluteDockerfilePath);
    const buildCommand = `docker build -f ${absoluteDockerfilePath} -t ${imageName} ${buildContextDir}`;

    // Execute the Docker build command
    exec(buildCommand, (error, stdout, stderr) => {
      if (error) {
        reject(`Error building Docker image: ${stderr}`);
        return;
      }

      // After building, inspect the image to get its sha256 hash
      const inspectCommand = `docker inspect --format="{{.Id}}" ${imageName}`;
      exec(inspectCommand, (inspectError, inspectStdout, inspectStderr) => {
        if (inspectError) {
          reject(`Error inspecting Docker image: ${inspectStderr}`);
          return;
        }

        // Extract the sha256 hash from the output and resolve the promise
        const imageId = inspectStdout.trim();
        if (imageId.startsWith("sha256:")) {
          resolve(imageId);
        } else {
          reject("Could not retrieve the sha256 hash of the Docker image.");
        }
      });
    });
  });
}

export async function startChromaContainer({
  authType,
}: {
  authType?: "basic" | "token" | "xtoken";
} = {}) {
  let container: GenericContainer;
  if (process.env.PREBUILT_CHROMADB_IMAGE) {
    container = new GenericContainer(process.env.PREBUILT_CHROMADB_IMAGE);
  } else {
    const imageHash = await buildDockerImage(
      path.join(BUILD_CONTEXT_DIR, "Dockerfile"),
      "chromadb-test",
    );
    container = new GenericContainer(imageHash);
    container = container.withCopyContentToContainer([
      {
        content: BASIC_AUTH_PASSWORD_FILE_CONTENTS,
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
