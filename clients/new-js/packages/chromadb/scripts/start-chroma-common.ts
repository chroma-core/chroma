import { GenericContainer, Wait } from "testcontainers";
import path from "node:path";
import { ChildProcess, execSync, spawn } from "node:child_process";
import chalk from "chalk";
import waitOn from "wait-on";
import { Readable } from "node:stream";

const CHROMADB_PORT = 8000;

const buildDockerImage = async (
  dockerfilePath: string,
  imageName: string,
): Promise<string> => {
  const absoluteDockerfilePath = path.resolve(dockerfilePath);
  const buildContextDir = path.dirname(absoluteDockerfilePath);
  const buildCommand = `docker build -f ${absoluteDockerfilePath} -t ${imageName} ${buildContextDir}`;

  try {
    console.log("Building docker image...");
    execSync(buildCommand, { stdio: "inherit" });

    // After building, inspect the image to get its sha256 hash
    const inspectCommand = `docker inspect --format="{{.Id}}" ${imageName}`;
    const imageId = execSync(inspectCommand, { encoding: "utf-8" }).trim();

    if (imageId.startsWith("sha256:")) {
      return imageId;
    }
  } catch (error) {
    throw new Error(`Error building Docker image: ${error}`);
  }
  throw new Error("Could not retrieve the sha256 hash of the Docker image.");
};

export const startContainer = async (
  buildContextDir: string,
  verbose?: boolean,
) => {
  let container: GenericContainer;
  if (process.env.PREBUILT_CHROMADB_IMAGE) {
    container = new GenericContainer(process.env.PREBUILT_CHROMADB_IMAGE);
  } else {
    const imageHash = await buildDockerImage(
      path.join(buildContextDir, "Dockerfile"),
      "chromadb-test",
    );
    container = new GenericContainer(imageHash)
      .withEnvironment({
        CHROMA_API_IMPL: "chromadb.api.segment.SegmentAPI",
      })
      .withLogConsumer((stream: Readable) => {
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

  container = container
    .withExposedPorts(CHROMADB_PORT)
    .withWaitStrategy(Wait.forListeningPorts())
    .withStartupTimeout(120_000)
    .withEnvironment(env);

  if (verbose) {
    container = container.withLogConsumer((stream: Readable) => {
      stream.on("data", (line) => console.log(line));
      stream.on("err", (line) => console.error(line));
      stream.on("end", () => console.log("Stream closed"));
    });
  }

  const startedContainer = await container.start();

  const chromaUrl = `http://${startedContainer.getHost()}:${startedContainer.getMappedPort(
    CHROMADB_PORT,
  )}`;

  return {
    url: chromaUrl,
    host: startedContainer.getHost(),
    port: startedContainer.getMappedPort(CHROMADB_PORT),
    container: startedContainer,
  };
};

export const startChromaServer = async (buildContextDir: string) => {
  const host = "127.0.0.1";
  const port = CHROMADB_PORT;
  const url = `http://${host}:${port}`;
  const heartbeatUrl = `${url}/api/v2/heartbeat`;
  let serverProcess: ChildProcess | null = null;

  console.log(chalk.blue("🚀 Building Rust binary..."));
  try {
    execSync("cargo build --bin chroma", { cwd: buildContextDir });
    console.log(chalk.green("✅ Rust binary built successfully."));
  } catch (e) {
    console.error(chalk.red(`Error building Rust binary: ${e}`));
    process.exit(1);
  }

  // jest throws an error if we use console.* after the test complete, so we use this flag to forward logs directly to stdout/stderr during shutdown
  let is_exiting = false;

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
      cwd: buildContextDir,
      stdio: "pipe", // Pipe stdio to control output logging if needed
      detached: true, // Run in detached mode to allow parent to exit independently if necessary
    },
  );

  serverProcess.stdout?.on("data", (data) => {
    const message = chalk.magenta(`🔧 rust-server: ${data.toString().trim()}`);
    if (is_exiting) {
      process.stdout.write(message + "\n");
    } else {
      console.log(message);
    }
  });

  serverProcess.stderr?.on("data", (data) => {
    const message = chalk.red(`🔧 rust-server-error: ${data.toString().trim()}`)
    if (is_exiting) {
      process.stderr.write(message + "\n");
    } else {
      console.error(message);
    }
  });

  console.log(chalk.yellow("⏳ Waiting for Chroma server heartbeat..."));

  try {
    await waitOn({
      resources: [heartbeatUrl],
      timeout: 30_000,
    });
  } catch (err) {
    console.error("Server failed to start in time:", err);
    serverProcess.kill();
    process.exit(1);
  }

  return {
    url,
    host,
    port,
    stop: () => {
      return new Promise<void>((resolve) => {
        if (!serverProcess) {
          resolve();
          return;
        }

        is_exiting = true;

        serverProcess.on('exit', () => {
          resolve();
        });

        serverProcess.kill();
      });
    }
  };
};
