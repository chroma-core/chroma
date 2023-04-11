import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { GetEmbeddingIncludeEnum } from '../src/generated';
import { IDS, DOCUMENTS, EMBEDDINGS, METADATAS } from './data';

test('it should get embedding with matching documents', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS, METADATAS, DOCUMENTS)

    const results = await collection.get(['test1'], undefined, undefined, undefined, [GetEmbeddingIncludeEnum.Embeddings, GetEmbeddingIncludeEnum.Metadatas, GetEmbeddingIncludeEnum.Documents])
    expect(results).toBeDefined()
    expect(results).toBeInstanceOf(Object)
    expect(results.embeddings[0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    await collection.update(
        ['test1'],
        [[1, 2, 3, 4, 5, 6, 7, 8, 9, 11]],
        [{ test: 'test1new' }],
        ["doc1new"]
    )

    const results2 = await collection.get(['test1'], undefined, undefined, undefined, [GetEmbeddingIncludeEnum.Embeddings, GetEmbeddingIncludeEnum.Metadatas, GetEmbeddingIncludeEnum.Documents])
    expect(results2).toBeDefined()
    expect(results2).toBeInstanceOf(Object)
    expect(results2.embeddings[0]).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 11])
    expect(results2.metadatas[0]).toEqual({ test: 'test1new' })
    expect(results2.documents[0]).toEqual('doc1new')
})