import { embeddingsToBase64Bytes } from "../src/utils";

const testCases = [
    {
        name: "encodes a single embedding",
        input: [[1.0, 2.0, 3.0]],
        expected: ["AACAPwAAAEAAAEBA"],
    },
    {
        name: "encodes multiple embeddings",
        input: [
            [1.5, 2.5],
            [3.5, 4.5],
            [5.5, 6.5],
        ],
        expected: ["AADAPwAAIEA=", "AABgQAAAkEA=", "AACwQAAA0EA="],
    },
    {
        name: "handles empty embedding array",
        input: [],
        expected: [],
    },
    {
        name: "handles empty embedding vector",
        input: [[]],
        expected: [""],
    },
    {
        name: "encodes negative numbers",
        input: [[-1.0, -2.5, -100.0]],
        expected: ["AACAvwAAIMAAAMjC"],
    },
    {
        name: "encodes zero",
        input: [[0.0, 0.0, 0.0]],
        expected: ["AAAAAAAAAAAAAAAA"],
    },
    {
        name: "encodes small decimals",
        input: [[0.1, 0.01, 0.001]],
        expected: ["zczMPQrXIzxvEoM6"],
    },
    {
        name: "encodes large numbers",
        input: [[1000.0, 1000000.0, 1e20]],
        expected: ["AAB6RAAkdEnseK1g"],
    },
    {
        name: "encodes mixed positive and negative",
        input: [[-0.5, 0.0, 0.5]],
        expected: ["AAAAvwAAAAAAAAA/"],
    },
    {
        name: "encodes very small decimals",
        input: [[1e-5, 1e-10, 1e-20]],
        expected: ["rMUnN//m2y4I5Twe"],
    },
    {
        name: "encodes high precision decimals",
        input: [[0.123456789, 0.987654321, 3.141592653589793]],
        expected: ["6tb8PerWfD/bD0lA"],
    },
    {
        name: "encodes repeating decimals",
        input: [[1 / 3, 2 / 3, 1 / 7]],
        expected: ["q6qqPquqKj8lSRI+"],
    },
    {
        name: "encodes numbers near float32 limits",
        input: [[3.4028235e38, 1.1754944e-38, -3.4028235e38]],
        expected: ["//9/fwAAgAD//3//"],
    },
    {
        name: "encodes single float (base64 double padding)",
        input: [[1.0]],
        expected: ["AACAPw=="],
    },
    {
        name: "encodes four floats",
        input: [[1.0, 2.0, 3.0, 4.0]],
        expected: ["AACAPwAAAEAAAEBAAACAQA=="],
    },
    {
        name: "encodes infinity",
        input: [[Infinity, -Infinity]],
        expected: ["AACAfwAAgP8="],
    },
    {
        name: "encodes subnormal floats",
        input: [[1.4e-45, -1.4e-45]],
        expected: ["AQAAAAEAAIA="],
    },
];

describe("embeddingsToBase64Bytes (Buffer implementation)", () => {
    for (const { name, input, expected } of testCases) {
        it(name, () => {
            expect(embeddingsToBase64Bytes(input)).toEqual(expected);
        });
    }
});

describe("embeddingsToBase64Bytes (portable implementation)", () => {
    for (const { name, input, expected } of testCases) {
        it(name, () => {
            const originalBuffer = globalThis.Buffer;
            jest.isolateModules(() => {
                // @ts-expect-error - intentionally removing Buffer to test portable path
                globalThis.Buffer = undefined;
                try {
                    const { embeddingsToBase64Bytes: portable } = require("../src/utils");
                    expect(portable(input)).toEqual(expected);
                } finally {
                    globalThis.Buffer = originalBuffer;
                }
            });
        });
    }
});
