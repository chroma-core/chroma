import {
    type SparseEmbeddingFunction,
    type SparseVector,
    registerSparseEmbeddingFunction,
} from "chromadb";
import { validateConfigSchema } from "@chroma-core/ai-embeddings-common";
import { newStemmer } from "snowball-stemmers";
import { DEFAULT_STOPWORDS } from "./stopwords";

const NAME = "chroma_bm25";

const DEFAULT_K = 1.2;
const DEFAULT_B = 0.75;
const DEFAULT_AVG_DOC_LEN = 256.0;
const DEFAULT_TOKEN_MAX_LENGTH = 40;

const DEFAULT_ENGLISH_STOPWORDS = DEFAULT_STOPWORDS as readonly string[];


export const DEFAULT_CHROMA_BM25_STOPWORDS = [...DEFAULT_ENGLISH_STOPWORDS];

type SnowballStemmer = {
    stem(token: string): string;
};

const ENGLISH_STEMMER: SnowballStemmer = newStemmer("english");

export interface ChromaBm25Args {
    k?: number;
    b?: number;
    avgDocLength?: number;
    tokenMaxLength?: number;
    stopwords?: string[];
}

export interface ChromaBm25Config {
    k?: number;
    b?: number;
    avg_doc_length?: number;
    token_max_length?: number;
    stopwords?: string[];
}

class Murmur3AbsHasher {
    constructor(private readonly seed = 0) { }

    private murmur3(key: string): number {
        let h1 = this.seed >>> 0;
        const c1 = 0xcc9e2d51;
        const c2 = 0x1b873593;
        const bytes = key.length - (key.length & 3);

        let i = 0;
        while (i < bytes) {
            let k1 =
                (key.charCodeAt(i) & 0xff) |
                ((key.charCodeAt(i + 1) & 0xff) << 8) |
                ((key.charCodeAt(i + 2) & 0xff) << 16) |
                ((key.charCodeAt(i + 3) & 0xff) << 24);
            i += 4;

            k1 = Math.imul(k1, c1);
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 = Math.imul(k1, c2);

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);
            h1 = Math.imul(h1, 5) + 0xe6546b64;
        }

        let k1 = 0;
        switch (key.length & 3) {
            case 3:
                k1 ^= (key.charCodeAt(i + 2) & 0xff) << 16;
            case 2:
                k1 ^= (key.charCodeAt(i + 1) & 0xff) << 8;
            case 1:
                k1 ^= key.charCodeAt(i) & 0xff;
                k1 = Math.imul(k1, c1);
                k1 = (k1 << 15) | (k1 >>> 17);
                k1 = Math.imul(k1, c2);
                h1 ^= k1;
        }

        h1 ^= key.length;
        h1 ^= h1 >>> 16;
        h1 = Math.imul(h1, 0x85ebca6b);
        h1 ^= h1 >>> 13;
        h1 = Math.imul(h1, 0xc2b2ae35);
        h1 ^= h1 >>> 16;

        return h1 >>> 0;
    }

    public hash(token: string): number {
        const unsigned = this.murmur3(token);
        const signed = (unsigned << 0) | 0;
        return Math.abs(signed);
    }
}

class Bm25Tokenizer {
    private readonly stopwords: ReadonlySet<string>;

    constructor(
        private readonly stemmer: SnowballStemmer,
        stopwords: Iterable<string>,
        private readonly tokenMaxLength: number,
    ) {
        this.stopwords = new Set(
            Array.from(stopwords, (word) => word.toLowerCase()),
        );
    }

    private removeNonAlphanumeric(text: string): string {
        return text.replace(/[^\p{L}\p{N}_\s]+/gu, " ");
    }

    private simpleTokenize(text: string): string[] {
        return text
            .toLowerCase()
            .split(/\s+/u)
            .filter(Boolean);
    }

    public tokenize(text: string): string[] {
        const cleaned = this.removeNonAlphanumeric(text);
        const rawTokens = this.simpleTokenize(cleaned);

        const tokens: string[] = [];
        for (const token of rawTokens) {
            if (token.length === 0) {
                continue;
            }

            if (this.stopwords.has(token)) {
                continue;
            }

            if (token.length > this.tokenMaxLength) {
                continue;
            }

            const stemmed = this.stemmer.stem(token).trim();

            if (stemmed.length > 0) {
                tokens.push(stemmed);
            }
        }

        return tokens;
    }
}

export class ChromaBm25EmbeddingFunction implements SparseEmbeddingFunction {
    public readonly name = NAME;

    private readonly tokenizer: Bm25Tokenizer;
    private readonly hasher: Murmur3AbsHasher;
    private readonly k: number;
    private readonly b: number;
    private readonly avgDocLength: number;
    private readonly tokenMaxLength: number;
    private readonly customStopwords?: string[];

    constructor(args: ChromaBm25Args = {}) {
        const {
            k = DEFAULT_K,
            b = DEFAULT_B,
            avgDocLength = DEFAULT_AVG_DOC_LEN,
            tokenMaxLength = DEFAULT_TOKEN_MAX_LENGTH,
            stopwords,
        } = args;

        this.k = k;
        this.b = b;
        this.avgDocLength = avgDocLength;
        this.tokenMaxLength = tokenMaxLength;
        this.customStopwords = stopwords ? [...stopwords] : undefined;

        const stopwordList =
            this.customStopwords ?? [...DEFAULT_ENGLISH_STOPWORDS];
        this.tokenizer = new Bm25Tokenizer(
            ENGLISH_STEMMER,
            stopwordList,
            tokenMaxLength,
        );

        this.hasher = new Murmur3AbsHasher();
    }

    private encode(text: string): SparseVector {
        const tokens = this.tokenizer.tokenize(text);

        if (tokens.length === 0) {
            return { indices: [], values: [] };
        }

        const docLen = tokens.length;
        const counts = new Map<number, number>();

        for (const token of tokens) {
            const tokenId = this.hasher.hash(token);
            counts.set(tokenId, (counts.get(tokenId) ?? 0) + 1);
        }

        const indices = Array.from(counts.keys()).sort((a, b) => a - b);
        const values = indices.map((idx) => {
            const tf = counts.get(idx)!;
            const denominator =
                tf +
                this.k *
                (1 - this.b + (this.b * docLen) / this.avgDocLength);
            return (tf * (this.k + 1)) / denominator;
        });

        return { indices, values };
    }

    public async generate(texts: string[]): Promise<SparseVector[]> {
        if (texts.length === 0) {
            return [];
        }

        return texts.map((text) => this.encode(text));
    }

    public async generateForQueries(texts: string[]): Promise<SparseVector[]> {
        return this.generate(texts);
    }

    public static buildFromConfig(config: ChromaBm25Config): ChromaBm25EmbeddingFunction {
        return new ChromaBm25EmbeddingFunction({
            k: config.k,
            b: config.b,
            avgDocLength: config.avg_doc_length,
            tokenMaxLength: config.token_max_length,
            stopwords: config.stopwords,
        });
    }

    public getConfig(): ChromaBm25Config {
        const config: ChromaBm25Config = {
            k: this.k,
            b: this.b,
            avg_doc_length: this.avgDocLength,
            token_max_length: this.tokenMaxLength,
        };

        if (this.customStopwords) {
            config.stopwords = [...this.customStopwords];
        }

        return config;
    }

    public validateConfigUpdate(newConfig: Record<string, unknown>): void {
        const mutableKeys = new Set(["k", "b", "avg_doc_length", "token_max_length", "stopwords"]);
        for (const key of Object.keys(newConfig)) {
            if (!mutableKeys.has(key)) {
                throw new Error(`Updating '${key}' is not supported for ${NAME}`);
            }
        }
    }

    public static validateConfig(config: ChromaBm25Config): void {
        validateConfigSchema(config, NAME);
    }
}

registerSparseEmbeddingFunction(NAME, ChromaBm25EmbeddingFunction);
