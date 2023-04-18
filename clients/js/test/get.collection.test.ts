import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { EMBEDDINGS, IDS, METADATAS } from './data';

test('it should get a collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS, METADATAS)
    const results = await collection.get(['test1'])
    expect(results).toBeDefined()
    expect(results).toBeInstanceOf(Object)
    expect(results.ids.length).toBe(1)
    expect(['test1']).toEqual(expect.arrayContaining(results.ids));
    expect(['test2']).not.toEqual(expect.arrayContaining(results.ids));

    const results2 = await collection.get(undefined, { 'test': 'test1' })
    expect(results2).toBeDefined()
    expect(results2).toBeInstanceOf(Object)
    expect(results2.ids.length).toBe(1)
    expect(['test1']).toEqual(expect.arrayContaining(results2.ids));
})

test('wrong code returns an error', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS, METADATAS)
    const results = await collection.get(undefined, { "test": { "$contains": "hello" } });
    expect(results.error).toBeDefined()
    expect(results.error).toContain("ValueError")
})

test('test gt, lt, in a simple small way', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS, METADATAS)
    const items = await collection.get(undefined, {"float_value": {"$gt": -1.4}})
    expect(items.ids.length).toBe(2)
    expect(['test2', 'test3']).toEqual(expect.arrayContaining(items.ids));
})