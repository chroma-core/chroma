import { describe, expect, test } from "@jest/globals";
import {
    DEFAULT_CHROMA_BM25_STOPWORDS,
    ChromaBm25EmbeddingFunction,
    type ChromaBm25Config,
} from "./index";

const isSorted = (arr: number[]): boolean => {
    for (let i = 1; i < arr.length; i += 1) {
        if (arr[i] < arr[i - 1]) {
            return false;
        }
    }
    return true;
};

describe("ChromaBm25EmbeddingFunction", () => {
    const embedder = new ChromaBm25EmbeddingFunction();

    test("matches comprehensive tokenization expectations", async () => {
        const [embedding] = await embedder.generate([
            "Usain Bolt's top speed reached ~27.8 mph (44.72 km/h)",
        ]);

        const expectedIndices = [
            230246813, 395514983, 458027949, 488165615, 729632045, 734978415,
            997512866, 1114505193, 1381820790, 1501587190, 1649421877,
            1837285388,
        ];
        const expectedValue = 1.6391153;

        expect(embedding.indices).toEqual(expectedIndices);
        embedding.values.forEach((value) => {
            expect(value).toBeCloseTo(expectedValue, 5);
        });
    });

    // mirrors rust test `test_bm25_stopwords_and_punctuation` to guarantee compatibility
    test("ensure Rust impl compatibilty", async () => {
        const [embedding] = await embedder.generate([
            "The   space-time   continuum   WARPS   near   massive   objects...",
        ]);

        const expectedIndices = [
            90097469, 519064992, 737893654, 1110755108, 1950894484, 2031641008,
            2058513491,
        ];
        const expectedValue = 1.660867;

        expect(embedding.indices).toEqual(expectedIndices);
        embedding.values.forEach((value) => {
            expect(value).toBeCloseTo(expectedValue, 5);
        });
    });

    test("generates consistent embeddings for multiple documents", async () => {
        const texts = [
            "Usain Bolt's top speed reached ~27.8 mph (44.72 km/h)",
            "The   space-time   continuum   WARPS   near   massive   objects...",
            "BM25 is great for sparse retrieval tasks",
        ];

        const embeddings = await embedder.generate(texts);

        expect(embeddings).toHaveLength(texts.length);
        embeddings.forEach((embedding, index) => {
            expect(embedding.indices.length).toBeGreaterThan(0);
            expect(embedding.values.length).toBe(embedding.indices.length);
            expect(isSorted(embedding.indices)).toBe(true);

            embedding.values.forEach((value) => {
                expect(value).toBeGreaterThan(0);
                expect(Number.isFinite(value)).toBe(true);
            });
        });
    });

    test("generateForQueries mirrors generate", async () => {
        const query = "retrieve BM25 docs";
        const [queryEmbedding] = await embedder.generateForQueries([query]);
        const [docEmbedding] = await embedder.generate([query]);

        expect(queryEmbedding.indices).toEqual(docEmbedding.indices);
        expect(queryEmbedding.values).toEqual(docEmbedding.values);
    });

    test("config round trip maintains settings", () => {
        const config = embedder.getConfig() as Required<ChromaBm25Config>;

        expect(config).toMatchObject({
            k: 1.2,
            b: 0.75,
            avg_doc_length: 256,
            token_max_length: 40,
        });
        expect(config.stopwords).toBeUndefined();

        const custom = ChromaBm25EmbeddingFunction.buildFromConfig({
            ...config,
            stopwords: DEFAULT_CHROMA_BM25_STOPWORDS.slice(0, 10),
        });

        const rebuiltConfig =
            custom.getConfig() as Required<ChromaBm25Config>;
        expect(rebuiltConfig.k).toBeCloseTo(config.k);
        expect(rebuiltConfig.b).toBeCloseTo(config.b);
        expect(rebuiltConfig.avg_doc_length).toBeCloseTo(config.avg_doc_length);
        expect(rebuiltConfig.token_max_length).toBe(config.token_max_length);
        expect(rebuiltConfig.stopwords).toEqual(
            DEFAULT_CHROMA_BM25_STOPWORDS.slice(0, 10),
        );
    });

    // A sparse index written by one client is read by whichever client queries
    // it next, so a term's id has to agree across the Python, Rust and
    // TypeScript implementations. Both of the others hash the token's UTF-8
    // bytes: mmh3.hash in
    // chromadb/utils/embedding_functions/schemas/bm25_tokenizer.py, and
    // token.as_bytes() in rust/chroma/src/embed/murmur3_abs_hasher.rs.
    //
    // Each expectation is abs(mmh3.hash(token, seed=0)) for the token as this
    // tokenizer produces it, that is lowercased and stemmed: "naïve" stems to
    // "naïv", and the stemmer leaves a character above U+00FF alone.
    test("non-ASCII tokens get the term ids python and rust produce", async () => {
        const cases: Array<[string, number]> = [
            ["café", 605818632],
            ["naïve", 756638508],
            ["Müller", 1613901631],
            ["Español", 589134877],
            ["日本語", 1515949417],
            ["Привет", 993413998],
        ];

        for (const [token, expected] of cases) {
            const [embedding] = await embedder.generate([token]);
            expect({ token, indices: embedding.indices }).toEqual({
                token,
                indices: [expected],
            });
        }
    });

    test("a non-ASCII document indexes and queries consistently", async () => {
        const [document] = await embedder.generate(["Le café est naïve"]);
        const [query] = await embedder.generateForQueries(["Le café est naïve"]);

        expect(query.indices).toEqual(document.indices);
    });
});
