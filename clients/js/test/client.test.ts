import { expect, test } from '@jest/globals';
import { ChromaClient } from '../src/index'
import chroma from './initClient'

test('it should create the client connection', async () => {
    expect(chroma).toBeDefined()
    expect(chroma).toBeInstanceOf(ChromaClient)
})

test('it should get the version', async () => {
    const version = await chroma.version()
    expect(version).toBeDefined()
    expect(version).toMatch(/^[0-9]+\.[0-9]+\.[0-9]+$/)
})

test('it should get the heartbeat', async () => {
    const heartbeat = await chroma.heartbeat()
    expect(heartbeat).toBeDefined()
    expect(heartbeat).toBeGreaterThan(0)
})

test('it should reset the database', async () => {
    await chroma.reset()
    const collections = await chroma.listCollections()
    expect(collections).toBeDefined()
    expect(collections).toBeInstanceOf(Array)
    expect(collections.length).toBe(0)
    
    const collection = await chroma.createCollection('test')
    const collections2 = await chroma.listCollections()
    expect(collections2).toBeDefined()
    expect(collections2).toBeInstanceOf(Array)
    expect(collections2.length).toBe(1)
    
    await chroma.reset()
    const collections3 = await chroma.listCollections()
    expect(collections3).toBeDefined()
    expect(collections3).toBeInstanceOf(Array)
    expect(collections3.length).toBe(0)
})
