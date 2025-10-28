declare module "snowball-stemmers" {
    export interface SnowballStemmer {
        stem(token: string): string;
    }

    export function newStemmer(language: string): SnowballStemmer;
    export const algorithms: string[];
}
