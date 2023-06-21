import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { CollectionItem, CollectionItems, IncludeEnum } from '../src/types';
import { addCollectionItems, asCollectionItems } from '../src/utils';


test('toCollection util should work', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection({ name: "test" });
    const ids = ['test1', 'test2', 'test3']
    const embeddings = [
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        [1, 2, 4, 4, 4, 6, 7, 8, 9, 10],
    ]

    const collectionItems: CollectionItems = [
        {
            id: ids[0],
            embedding: embeddings[0]
        },
        {
            id: ids[1],
            embedding: embeddings[1]
        }
    ]

    await collection.add(addCollectionItems(collectionItems))
    const count = await collection.count()
    expect(count).toBe(2)

    const newItem: CollectionItem = {
        id: ids[2],
        embedding: embeddings[2]
    }

    await collection.add(addCollectionItems(newItem))
    const count2 = await collection.count()
    expect(count2).toBe(3)
})

test('test asCollectionItems util', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection({ name: "test" });
    const ids = ['test1', 'test2', 'test3']
    const embeddings = [
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        [1, 2, 4, 4, 4, 6, 7, 8, 9, 10],
    ]
    const documents = ['This is a test', 'This is another test', 'This is a test']
    const metadatas = [{ test: 'test' }, { test: 'test' }, { test: 'test' }]

    const collectionItems: CollectionItems = [
        {
            id: ids[0],
            embedding: embeddings[0],
            document: documents[0],
            metadata: metadatas[0]
        },
        {
            id: ids[1],
            embedding: embeddings[1],
            document: documents[1],
            metadata: metadatas[1]
        }
    ]

    await collection.add(addCollectionItems(collectionItems))
    const count = await collection.count()
    expect(count).toBe(2)

    const result = asCollectionItems(await collection.query({
        queryEmbeddings: embeddings[0],
        nResults: 3,
        include: [IncludeEnum.Embeddings, IncludeEnum.Documents, IncludeEnum.Metadatas, IncludeEnum.Distances]
    }))

    expect(result).toBeDefined()
    expect(result).toBeInstanceOf(Array)
    expect(result.length).toBe(1)
    expect(result[0].length).toBe(2)
    expect(result[0][0].metadata).toEqual(metadatas[0])
})
