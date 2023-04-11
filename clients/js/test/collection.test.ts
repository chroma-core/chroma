import { expect, test } from '@jest/globals';
import chroma from './initClient'

test('it should modify collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    expect(collection.name).toBe('test')

    await collection.modify('test2')
    expect(collection.name).toBe('test2')
})

test('it should store metadata', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test', { test: 'test' })
    expect(collection.metadata).toEqual({ test: 'test' })

    // get the collection
    const collection2 = await chroma.getCollection('test')
    expect(collection2.metadata).toEqual({ test: 'test' })

    // get or create the collection
    const collection3 = await chroma.getOrCreateCollection('test')
    expect(collection3.metadata).toEqual({ test: 'test' })

    // modify
    await collection3.modify(undefined, { test: 'test2' })
    expect(collection3.metadata).toEqual({ test: 'test2' })

    // get it again 
    const collection4 = await chroma.getCollection('test')
    expect(collection4.metadata).toEqual({ test: 'test2' })
})