import { describe, expect, test } from "@jest/globals";
import { ChromaValueError } from "../src/errors";
import {
  validateBaseRecordSet,
  validateIDs,
  validateMetadata,
} from "../src/utils";

describe("validateMetadata", () => {
  test("accepts scalar values", () => {
    expect(() =>
      validateMetadata({ str: "hello", num: 42, bool: true }),
    ).not.toThrow();
  });

  test("accepts null values", () => {
    expect(() => validateMetadata({ key: null })).not.toThrow();
  });

  test("accepts homogeneous string arrays", () => {
    expect(() =>
      validateMetadata({ tags: ["action", "comedy", "drama"] }),
    ).not.toThrow();
  });

  test("accepts homogeneous number arrays", () => {
    expect(() => validateMetadata({ scores: [1, 2, 3] })).not.toThrow();
  });

  test("accepts homogeneous boolean arrays", () => {
    expect(() =>
      validateMetadata({ flags: [true, false, true] }),
    ).not.toThrow();
  });

  test("rejects empty arrays", () => {
    expect(() => validateMetadata({ tags: [] })).toThrow(
      "Expected metadata list value for key 'tags' to be non-empty",
    );
  });

  test("rejects mixed-type arrays (string + number)", () => {
    expect(() => validateMetadata({ mixed: ["hello", 42] as any })).toThrow(
      "Expected metadata list value for key 'mixed' to contain only the same type",
    );
  });

  test("rejects mixed-type arrays (number + boolean)", () => {
    expect(() => validateMetadata({ mixed: [1, true] as any })).toThrow(
      "Expected metadata list value for key 'mixed' to contain only the same type",
    );
  });

  test("rejects arrays with non-scalar elements", () => {
    expect(() => validateMetadata({ nested: [{ a: 1 }] as any })).toThrow(
      "Expected metadata list value for key 'nested' to contain only strings, numbers, or booleans",
    );
  });

  test("rejects arrays containing null", () => {
    expect(() => validateMetadata({ vals: [null, "a"] as any })).toThrow(
      "Expected metadata list value for key 'vals' to contain only strings, numbers, or booleans",
    );
  });

  test("rejects empty metadata", () => {
    expect(() => validateMetadata({})).toThrow(
      "Expected metadata to be non-empty",
    );
  });

  test("rejects invalid non-scalar, non-array values", () => {
    expect(() =>
      validateMetadata({ bad: { nested: "object" } as any }),
    ).toThrow(
      "Expected metadata value for key 'bad' to be a string, number, boolean",
    );
  });

  test("skips validation for undefined metadata", () => {
    expect(() => validateMetadata(undefined)).not.toThrow();
  });

  test("accepts metadata with mixed scalar and array fields", () => {
    expect(() =>
      validateMetadata({
        name: "test",
        count: 5,
        active: true,
        tags: ["a", "b"],
        scores: [1, 2, 3],
      }),
    ).not.toThrow();
  });
});

describe("validateBaseRecordSet documents", () => {
  test("accepts string documents, including the empty string", () => {
    expect(() =>
      validateBaseRecordSet({ recordSet: { documents: ["a", ""] } }),
    ).not.toThrow();
  });

  test("rejects number documents", () => {
    const run = () =>
      validateBaseRecordSet({ recordSet: { documents: [123 as any] } });
    expect(run).toThrow(ChromaValueError);
    expect(run).toThrow("Expected each document to be a string");
  });

  test("rejects object documents", () => {
    expect(() =>
      validateBaseRecordSet({ recordSet: { documents: [{} as any] } }),
    ).toThrow(ChromaValueError);
  });

  test("rejects a non-string document mixed with strings", () => {
    expect(() =>
      validateBaseRecordSet({ recordSet: { documents: ["a", 1 as any] } }),
    ).toThrow(ChromaValueError);
  });

  test("still rejects null documents", () => {
    expect(() =>
      validateBaseRecordSet({ recordSet: { documents: [null as any] } }),
    ).toThrow(ChromaValueError);
  });
});

describe("validateIDs", () => {
  test("accepts unique ids", () => {
    expect(() => validateIDs(["a", "b", ""])).not.toThrow();
  });

  test("rejects duplicate ids", () => {
    expect(() => validateIDs(["a", "a"])).toThrow(ChromaValueError);
    expect(() => validateIDs(["a", "a"])).toThrow(
      "Expected IDs to be unique, but found duplicates of a",
    );
  });

  test("rejects duplicate empty-string ids", () => {
    expect(() => validateIDs(["", ""])).toThrow(ChromaValueError);
    expect(() => validateIDs(["a", "", "b", ""])).toThrow(
      "Expected IDs to be unique",
    );
  });
});
