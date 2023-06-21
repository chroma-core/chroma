import { Api } from "./generated"
import Count200Response = Api.Count200Response;
import { CollectionItem, CollectionItems, ID, IDs, Embeddings, Documents, Metadatas, QueryResponse } from "./types";
import { Collection } from "./Collection";

// a function to convert a non-Array object to an Array
export function toArray<T>(obj: T | Array<T>): Array<T> {
    if (Array.isArray(obj)) {
        return obj;
    } else {
        return [obj];
    }
}

// a function to convert an array to array of arrays
export function toArrayOfArrays<T>(obj: Array<Array<T>> | Array<T>): Array<Array<T>> {
    if (Array.isArray(obj[0])) {
        return obj as Array<Array<T>>;
    } else {
        return [obj] as Array<Array<T>>;
    }
}

// we need to override constructors to make it work with jest
// https://stackoverflow.com/questions/76007003/jest-tobeinstanceof-expected-constructor-array-received-constructor-array
export function repack(value: unknown): any {
    if (Boolean(value) && typeof value === "object") {
        if (Array.isArray(value)) {
            return new Array(...value);
        } else {
            return { ...value };
        }
    } else {
        return value;
    }
}

export async function handleError(error: unknown) {
    if (error instanceof Response) {
        try {
            const res = await error.json();
            if ("error" in res) {
                return { error: res.error };
            }
        } catch (e: unknown) {
            return {
                //@ts-ignore
                error:
                    e && typeof e === "object" && "message" in e
                        ? e.message
                        : "unknown error",
            };
        }
    }
    return { error };
}

export async function handleSuccess(response: Response | string | Count200Response) {
    switch (true) {
        case response instanceof Response:
            return repack(await (response as Response).json());
        case typeof response === "string":
            return repack((response as string)); // currently version is the only thing that return non-JSON
        default:
            return repack(response);
    }
}

export function addCollectionItems(items: CollectionItem | CollectionItems): {
    ids: IDs,
    embeddings: Embeddings,
    documents: Documents,
    metadatas: Metadatas,
} {
    const ids: IDs = [];
    const embeddings: Embeddings = [];
    const documents: Documents = [];
    const metadatas: Metadatas = [];

    if (!Array.isArray(items)) {
        items = [items];
    }

    for (const item of items) {
        ids.push(item.id);
        embeddings.push(item.embedding || []);
        documents.push(item.document || "");
        metadatas.push(item.metadata || {});
    }

    return { ids, embeddings, documents, metadatas };
}

export function asCollectionItems(queryResponse: QueryResponse): CollectionItems[] {
    const queryResponseItems: CollectionItems[] = [];
    for (let i = 0; i < queryResponse.ids.length; i++) {
        const items: CollectionItems = [];
        for (let j = 0; j < queryResponse.ids[i].length; j++) {
            var collectionItem: CollectionItem = {
                id: queryResponse.ids[i][j],
                embedding: queryResponse.embeddings ? queryResponse.embeddings[i][j] : null,
                document: queryResponse.documents ? queryResponse.documents[i][j] : null,
                metadata: queryResponse.metadatas ? queryResponse.metadatas[i][j] : null,
                distance: queryResponse.distances ? queryResponse.distances[i][j] : null,
            };
            items.push(collectionItem);
        }
        queryResponseItems.push(items);
    }
    return queryResponseItems;
}
