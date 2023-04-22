import { expect, test } from '@jest/globals';
import chroma from './initClient'
import { IDS, EMBEDDINGS } from './data';

test('it should peek a collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    await collection.add(IDS, EMBEDDINGS)
    const results = await collection.peek(2)
    expect(results).toBeDefined()
    expect(results).toBeInstanceOf(Object)
    expect(results.ids.length).toBe(2)
    expect(['test1', 'test2']).toEqual(expect.arrayContaining(results.ids));
})
