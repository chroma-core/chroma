import { expect, test } from '@jest/globals';
import { ChromaClient } from '../src/index'
import chroma from './initClient'

test('it should list collections', async () => {
    await chroma.reset()
    let collections = await chroma.listCollections()
    expect(collections).toBeDefined()
    expect(collections).toBeInstanceOf(Array)
    expect(collections.length).toBe(0)
    const collection = await chroma.createCollection('test')
    collections = await chroma.listCollections()
    expect(collections.length).toBe(1)
})

test('it should create a collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    expect(collection).toBeDefined()
    expect(collection).toHaveProperty('name')
    let collections = await chroma.listCollections()
    expect([{ name: 'test', metadata: null }]).toEqual(expect.arrayContaining(collections));
    expect([{ name: 'test2', metadata: null }]).not.toEqual(expect.arrayContaining(collections));
})

test('it should get a collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    const collection2 = await chroma.getCollection('test')
    expect(collection).toBeDefined()
    expect(collection2).toBeDefined()
    expect(collection).toHaveProperty('name')
    expect(collection2).toHaveProperty('name')
    expect(collection.name).toBe(collection2.name)
})

test('it should get or create a collection', async () => {
    await chroma.reset()
    await chroma.createCollection('test')

    const collection2 = await chroma.getOrCreateCollection('test')
    expect(collection2).toBeDefined()
    expect(collection2).toHaveProperty('name')
    expect(collection2.name).toBe('test')

    const collection3 = await chroma.getOrCreateCollection('test3')
    expect(collection3).toBeDefined()
    expect(collection3).toHaveProperty('name')
    expect(collection3.name).toBe('test3')
})

test('it should delete a collection', async () => {
    await chroma.reset()
    const collection = await chroma.createCollection('test')
    let collections = await chroma.listCollections()
    expect(collections.length).toBe(1)
    await chroma.deleteCollection('test')
    collections = await chroma.listCollections()
    expect(collections.length).toBe(0)
})

// TODO: I want to test this, but I am not sure how to
// test('custom index params', async () => {
//     throw new Error('not implemented')
//     await chroma.reset()
//     const collection = await chroma.createCollection('test', {"hnsw:space": "cosine"})
// })