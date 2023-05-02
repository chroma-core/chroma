import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { DOCUMENTS, EMBEDDINGS, IDS } from './data';
import { GetEmbeddingIncludeEnum } from '../src/generated';
import { METADATAS } from './data';

test('it should return an error when inserting an ID that alreay exists in the Collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS, METADATAS)
    const results = await collection.add(IDS, EMBEDDINGS, METADATAS);
    expect(results.error).toBeDefined()
    expect(results.error).toContain("IDAlreadyExistsError")
})

test('It should return an error when inserting duplicate IDs in the same batch', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    const ids = IDS.concat(["test1"])
    const embeddings = EMBEDDINGS.concat([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]])
    const metadatas = METADATAS.concat([{ test: 'test1', 'float_value': 0.1 }])
    try {
        await collection.add(ids, embeddings, metadatas);
    } catch (e: any) {
        expect(e.message).toMatch('duplicates')
    }
})