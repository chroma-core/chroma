import path from "node:path";
import { exec, spawn, ChildProcess } from "node:child_process";
import { GenericContainer, Wait } from "testcontainers";
import bcrypt from "bcrypt";
import chalk from "chalk";
import http from "node:http";

const CHROMADB_PORT = 8000;

const BUILD_CONTEXT_DIR = path.join(__dirname, "../../../../..");

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
    container = container.withEnvironment({
      CHROMA_API_IMPL: "chromadb.api.segment.SegmentAPI",
    });
    container.withLogConsumer((stream) => {
      stream.on("data", (line: Buffer) => {
        console.log(
          chalk.blue("🐳 chromadb: ") + line.toString("utf-8").trimEnd(),
        );
      });
    });
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

/**
 * Starts the Chroma server using the Rust binary.
 * Waits for it to be available via heartbeat check.
 * Returns connection details and a stop function.
 */
export async function startChromaRustServer(): Promise<{
  url: string;
  host: string;
  port: number;
  stop: () => Promise<void>;
}> {
  const host = "127.0.0.1";
  const port = CHROMADB_PORT;
  const url = `http://${host}:${port}`;
  const heartbeatUrl = `${url}/api/v2/heartbeat`;
  let serverProcess: ChildProcess | null = null;

  console.log(chalk.blue("🚀 Building Rust binary..."));
  await new Promise<void>((resolve, reject) => {
    exec(
      "cargo build --bin chroma",
      { cwd: BUILD_CONTEXT_DIR },
      (error, stdout, stderr) => {
        if (error) {
          console.error(chalk.red(`Error building Rust binary: ${stderr}`));
          reject(error);
        } else {
          console.log(chalk.green("✅ Rust binary built successfully."));
          // console.log(stdout); // Optional: log build output
          resolve();
        }
      },
    );
  });

  console.log(chalk.blue("🚀 Starting Rust Chroma server..."));
  serverProcess = spawn(
    "cargo",
    [
      "run",
      "--bin",
      "chroma",
      "--",
      "run",
      "bin/rust_single_node_integration_test_config.yaml",
    ],
    {
      cwd: BUILD_CONTEXT_DIR,
      stdio: "pipe", // Pipe stdio to control output logging if needed
      detached: true, // Run in detached mode to allow parent to exit independently if necessary
    },
  );

  // Optional: Log server output/errors
  serverProcess.stdout?.on("data", (data) => {
    console.log(chalk.magenta(`🔧 rust-server: ${data.toString().trim()}`));
  });
  serverProcess.stderr?.on("data", (data) => {
    console.error(chalk.red(`🔧 rust-server-error: ${data.toString().trim()}`));
  });

  console.log(chalk.yellow("⏳ Waiting for Chroma server heartbeat..."));
  let attempts = 0;
  const maxAttempts = 10;
  const initialRetryDelay = 1000;
  const maxRetryDelay = 10000;

  while (attempts < maxAttempts) {
    attempts++;
    try {
      await new Promise<void>((resolve, reject) => {
        const req = http.get(heartbeatUrl, (res) => {
          if (res.statusCode === 200) {
            res.resume(); // Consume response data to free up memory
            resolve();
          } else {
            res.resume();
            reject(
              new Error(`Heartbeat failed with status: ${res.statusCode}`),
            );
          }
        });
        req.on("error", (err) => reject(err));
        req.setTimeout(900, () => {
          // Shorter timeout for quick retries
          req.destroy(new Error("Heartbeat request timed out"));
        });
      });
      console.log(chalk.green("✅ Chroma server is up!"));
      break; // Exit loop on success
    } catch (error: any) {
      if (attempts >= maxAttempts) {
        console.error(
          chalk.red(
            `❌ Chroma server failed to start after ${maxAttempts} attempts. Last error: ${error.message}`,
          ),
        );
        if (serverProcess?.pid) {
          // Ensure the process is killed if startup fails
          try {
            process.kill(-serverProcess.pid); // Kill the process group
          } catch (killError) {
            console.error(
              chalk.red(`Error killing server process: ${killError}`),
            );
          }
        }
        throw new Error("Chroma server failed to start within the timeout.");
      }
      const currentDelay = Math.min(
        initialRetryDelay * 2 ** (attempts - 1),
        maxRetryDelay,
      );
      console.log(
        chalk.yellow(
          `Attempt ${attempts}/${maxAttempts} failed. Retrying in ${
            currentDelay / 1000
          }s...`,
        ),
      );
      await new Promise((resolve) => setTimeout(resolve, currentDelay));
    }
  }

  const stop = async (): Promise<void> => {
    return new Promise((resolve, reject) => {
      if (serverProcess && serverProcess.pid) {
        console.log(
          chalk.blue(
            `🛑 Stopping Chroma server (PID: ${serverProcess.pid})...`,
          ),
        );
        // Kill the entire process group using the negative PID
        try {
          const killed = process.kill(-serverProcess.pid, "SIGTERM"); // Send SIGTERM to the process group
          if (killed) {
            console.log(
              chalk.green("✅ Chroma server process signaled to stop."),
            );
            // Optionally wait for the process to exit fully
            serverProcess.on("exit", (code, signal) => {
              console.log(
                chalk.blue(
                  `Chroma server process exited with code ${code}, signal ${signal}`,
                ),
              );
              resolve();
            });
            serverProcess.on("error", (err) => {
              // Handle potential errors during exit
              console.error(
                chalk.red(`Error during server process exit: ${err}`),
              );
              reject(err);
            });
          } else {
            console.warn(
              chalk.yellow(
                "Signal could not be sent to server process (it might have already exited).",
              ),
            );
            resolve(); // Resolve even if signal fails, assuming it's gone
          }
        } catch (err: any) {
          console.error(
            chalk.red(`Error stopping server process: ${err.message}`),
          );
          // If the error is ESRCH, the process likely already exited
          if (err.code === "ESRCH") {
            console.log(chalk.yellow("Server process likely already exited."));
            resolve();
          } else {
            reject(err);
          }
        }
      } else {
        console.log(chalk.yellow("No server process reference found to stop."));
        resolve();
      }
    });
  };

  return {
    url,
    host,
    port,
    stop,
  };
}
