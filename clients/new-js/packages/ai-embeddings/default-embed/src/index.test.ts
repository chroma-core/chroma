import { DefaultEmbeddingFunction, MAX_CACHED_PIPELINES } from "./index";
import { pipeline } from "@huggingface/transformers";
import { beforeEach, describe, expect, it, jest } from "@jest/globals";

// Mock the transformers pipeline
jest.mock("@huggingface/transformers", () => {
  // Create a mock embeddings result
  const mockEmbeddings = [
    Array(384)
      .fill(0)
      .map((_, i) => i / 1000),
    Array(384)
      .fill(0)
      .map((_, i) => (i + 100) / 1000),
  ];

  // Create the pipeline mock that returns a function
  const pipelineFunction = jest.fn().mockImplementation(async () => {
    // When the pipeline result is called with text, it returns this object with tolist
    return function (texts: string[], options: any) {
      return {
        tolist: () => mockEmbeddings,
      };
    };
  });

  return {
    pipeline: pipelineFunction,
  };
});

describe("DefaultEmbeddingFunction", () => {
  let embedder: DefaultEmbeddingFunction;

  beforeEach(() => {
    embedder = new DefaultEmbeddingFunction();
  });

  it("should initialize with default parameters", () => {
    expect(embedder.name).toBe("default");
    expect(embedder.getConfig().model_name).toBe("Xenova/all-MiniLM-L6-v2");
    expect(embedder.getConfig().revision).toBe("main");
    expect(embedder.getConfig().dtype).toBe("fp32");
  });

  it("should initialize with custom parameters", () => {
    const customEmbedder = new DefaultEmbeddingFunction({
      modelName: "custom-model",
      revision: "custom-revision",
      dtype: "fp16",
    });

    expect(customEmbedder.getConfig().model_name).toBe("custom-model");
    expect(customEmbedder.getConfig().revision).toBe("custom-revision");
    expect(customEmbedder.getConfig().dtype).toBe("fp16");
  });

  it("should handle deprecated quantized parameter", () => {
    const quantizedEmbedder = new DefaultEmbeddingFunction({
      quantized: true,
    });

    expect(quantizedEmbedder.getConfig().dtype).toBe("uint8");
  });

  it("should generate embeddings with correct dimensions", async () => {
    const texts = ["Hello world", "Test text"];
    const embeddings = await embedder.generate(texts);

    // Verify we got the correct number of embeddings
    expect(embeddings.length).toBe(texts.length);

    // Verify each embedding has the correct dimension (384 for MiniLM-L6-v2)
    embeddings.forEach((embedding) => {
      expect(embedding.length).toBe(384);
    });

    // Verify embeddings are different (this works with our mock implementation)
    const [embedding1, embedding2] = embeddings;
    expect(embedding1).not.toEqual(embedding2);
  });

  it("should build from config", () => {
    const config = {
      model_name: "config-model",
      revision: "config-revision",
      dtype: "q8" as const,
    };

    const configEmbedder = DefaultEmbeddingFunction.buildFromConfig(config);

    expect(configEmbedder.getConfig().model_name).toBe("config-model");
    expect(configEmbedder.getConfig().revision).toBe("config-revision");
    expect(configEmbedder.getConfig().dtype).toBe("q8");
  });

  it("should validate config updates", () => {
    const newConfig = { model_name: "model-2" };

    expect(() => {
      new DefaultEmbeddingFunction({
        modelName: "model-1",
      }).validateConfigUpdate(newConfig);
    }).toThrow(
      "The DefaultEmbeddingFunction's 'model' cannot be changed after initialization.",
    );
  });

  describe("pipeline caching", () => {
    const pipelineMock = pipeline as unknown as jest.Mock<
      (...args: unknown[]) => Promise<unknown>
    >;

    beforeEach(() => {
      pipelineMock.mockClear();
    });

    it("should load the pipeline once and reuse it across calls", async () => {
      const embedder = new DefaultEmbeddingFunction({
        modelName: "reuse-across-calls",
      });

      await embedder.generate(["first"]);
      await embedder.generate(["second"]);
      await Promise.all([
        embedder.generate(["third"]),
        embedder.generate(["fourth"]),
      ]);

      expect(pipelineMock).toHaveBeenCalledTimes(1);
    });

    it("should share the pipeline across instances with the same config", async () => {
      await new DefaultEmbeddingFunction({
        modelName: "shared-config",
      }).generate(["a"]);
      await DefaultEmbeddingFunction.buildFromConfig({
        model_name: "shared-config",
      }).generate(["b"]);

      expect(pipelineMock).toHaveBeenCalledTimes(1);
    });

    it("should load separate pipelines for different configs", async () => {
      await new DefaultEmbeddingFunction({
        modelName: "separate-config",
        dtype: "fp32",
      }).generate(["a"]);
      await new DefaultEmbeddingFunction({
        modelName: "separate-config",
        dtype: "q8",
      }).generate(["b"]);

      expect(pipelineMock).toHaveBeenCalledTimes(2);
    });

    it("should retry loading after a failed load", async () => {
      const embedder = new DefaultEmbeddingFunction({
        modelName: "retry-after-failure",
      });
      pipelineMock.mockImplementationOnce(async () => {
        throw new Error("download failed");
      });

      await expect(embedder.generate(["a"])).rejects.toThrow("download failed");
      await expect(embedder.generate(["b"])).resolves.toHaveLength(2);
      expect(pipelineMock).toHaveBeenCalledTimes(2);
    });

    it("should evict the least recently used pipeline beyond the cache limit", async () => {
      const load = (name: string) =>
        new DefaultEmbeddingFunction({ modelName: name }).generate(["x"]);
      const names = Array.from(
        { length: MAX_CACHED_PIPELINES },
        (_, i) => `lru-${i}`,
      );

      for (const name of names) await load(name);
      await load(names[0]);
      await load("lru-overflow");
      expect(pipelineMock).toHaveBeenCalledTimes(MAX_CACHED_PIPELINES + 1);

      pipelineMock.mockClear();
      await load(names[0]);
      await load("lru-overflow");
      expect(pipelineMock).not.toHaveBeenCalled();

      await load(names[1]);
      expect(pipelineMock).toHaveBeenCalledTimes(1);
    });

    describe("with delayed loads", () => {
      const load = (name: string) =>
        new DefaultEmbeddingFunction({ modelName: name }).generate(["x"]);
      const delayLoads = (count: number) => {
        const finishers: (() => void)[] = [];
        for (let i = 0; i < count; i++) {
          pipelineMock.mockImplementationOnce(
            () =>
              new Promise((resolve) => {
                finishers.push(() => resolve(() => ({ tolist: () => [[0]] })));
              }),
          );
        }
        return () => finishers.forEach((finish) => finish());
      };

      it("should share in-flight loads beyond the cache limit", async () => {
        const finishAll = delayLoads(MAX_CACHED_PIPELINES + 1);
        const names = Array.from(
          { length: MAX_CACHED_PIPELINES + 1 },
          (_, i) => `in-flight-${i}`,
        );
        const loads = names.map(load);
        loads.push(load(names[0]));
        expect(pipelineMock).toHaveBeenCalledTimes(MAX_CACHED_PIPELINES + 1);

        finishAll();
        await Promise.all(loads);

        // Once loaded, the limit applies again: the least recently used
        // pipeline was evicted and the re-requested first one was kept.
        pipelineMock.mockClear();
        await load(names[0]);
        expect(pipelineMock).not.toHaveBeenCalled();
        await load(names[1]);
        expect(pipelineMock).toHaveBeenCalledTimes(1);
      });

      it("should not evict loaded pipelines for an in-flight load", async () => {
        const names = Array.from(
          { length: MAX_CACHED_PIPELINES },
          (_, i) => `loaded-${i}`,
        );
        for (const name of names) await load(name);
        const finish = delayLoads(1);
        const pending = load("loaded-pending");

        pipelineMock.mockClear();
        for (const name of names) await load(name);
        expect(pipelineMock).not.toHaveBeenCalled();

        finish();
        await pending;
      });
    });

    describe("progress callbacks", () => {
      const pipe = () => ({ tolist: () => [[0]] });
      let emit: (info: unknown) => void;
      let finishLoad: () => void;

      beforeEach(() => {
        pipelineMock.mockImplementationOnce(
          (_task, _model, options) =>
            new Promise((resolve) => {
              emit = (options as { progress_callback: (info: unknown) => void })
                .progress_callback;
              finishLoad = () => resolve(pipe);
            }),
        );
      });

      it("should send load progress to every instance waiting on it", async () => {
        const first = jest.fn();
        const second = jest.fn();
        const loads = [
          new DefaultEmbeddingFunction({
            modelName: "progress-shared",
            progressCallback: first,
          }).generate(["a"]),
          new DefaultEmbeddingFunction({
            modelName: "progress-shared",
            progressCallback: second,
          }).generate(["b"]),
        ];

        emit({ status: "progress", progress: 50 });
        finishLoad();
        await Promise.all(loads);

        expect(pipelineMock).toHaveBeenCalledTimes(1);
        expect(first).toHaveBeenCalledWith({
          status: "progress",
          progress: 50,
        });
        expect(second).toHaveBeenCalledWith({
          status: "progress",
          progress: 50,
        });
      });

      it("should keep notifying others when one callback throws", async () => {
        const healthy = jest.fn();
        const loads = [
          new DefaultEmbeddingFunction({
            modelName: "progress-throwing",
            progressCallback: () => {
              throw new Error("callback failed");
            },
          }).generate(["a"]),
          new DefaultEmbeddingFunction({
            modelName: "progress-throwing",
            progressCallback: healthy,
          }).generate(["b"]),
        ];

        emit({ status: "progress" });
        finishLoad();

        await expect(Promise.all(loads)).resolves.toHaveLength(2);
        expect(healthy).toHaveBeenCalledTimes(1);
      });

      it("should not retain callbacks after the load settles", async () => {
        const early = jest.fn();
        const late = jest.fn();
        const loading = new DefaultEmbeddingFunction({
          modelName: "progress-settled",
          progressCallback: early,
        }).generate(["a"]);
        finishLoad();
        await loading;

        await new DefaultEmbeddingFunction({
          modelName: "progress-settled",
          progressCallback: late,
        }).generate(["b"]);
        emit({ status: "progress" });

        expect(early).not.toHaveBeenCalled();
        expect(late).not.toHaveBeenCalled();
      });
    });
  });
});
