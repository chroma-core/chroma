import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { QueryEmbeddingIncludeEnum } from '../src/generated';
import { EMBEDDINGS, IDS, METADATAS, DOCUMENTS } from './data';

test('it should query a collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS)
    const results = await collection.query([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
    expect(results).toBeDefined()
    expect(results).toBeInstanceOf(Object)
    expect(['test1', 'test2']).toEqual(expect.arrayContaining(results.ids[0]));
    expect(['test3']).not.toEqual(expect.arrayContaining(results.ids[0]));
})

// test where_document
test('it should get embedding with matching documents', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS, METADATAS, DOCUMENTS)

    const results = await collection.query([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3, undefined, undefined, { "$contains": "This is a test" })

    // it should only return doc1 
    expect(results).toBeDefined()
    expect(results).toBeInstanceOf(Object)
    expect(results.ids.length).toBe(1)
    expect(['test1']).toEqual(expect.arrayContaining(results.ids[0]));
    expect(['test2']).not.toEqual(expect.arrayContaining(results.ids[0]));
    expect(['This is a test']).toEqual(expect.arrayContaining(results.documents[0]));

    const results2 = await collection.query([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3, undefined, undefined, { "$contains": "This is a test" }, [QueryEmbeddingIncludeEnum.Embeddings])

    expect(results2.embeddings[0][0]).toBeInstanceOf(Array)
    expect(results2.embeddings[0].length).toBe(1)
    expect(results2.embeddings[0][0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
})

