// import env.ts
import { chromaClient } from '../chromaClient';

window.onload = async () => {

  // test reset
  await chromaClient.reset()
  
  // test create collection
  try {
    const newCollection = await chromaClient.createCollection({
      createCollection: { name: "created-from-js" }
    })
    console.log(newCollection);
  } catch (error) {
    console.error(error);
  }
  
  // test list collections
  let collections = await chromaClient.listCollections();

  // test get collection
  const collection = await chromaClient.getCollection({
    collectionName: "created-from-js"
  });

  // test add on collection

  // do this 20 times
  for (let i = 0; i < 20; i++) {
    await chromaClient.add({
      collectionName: "created-from-js",
      addEmbedding: {
        embeddings: [1, 2, 3, 4, 5],
        ids: "test-id-" + i.toString(),
        increment_index: false
      },
    
    })
  }

  await chromaClient.createIndex({collectionName: "created-from-js"})

  // test count on collection
  let countObject = await chromaClient.count({collectionName: "created-from-js"})
  let count = JSON.parse(countObject.data)
  

  // test get on collection
  let getData = await chromaClient.get({collectionName: "created-from-js", getEmbedding: {limit: 5}})

  // test query on collection
  let queryData = await chromaClient.getNearestNeighbors({collectionName: "created-from-js", queryEmbedding: {query_embeddings: [3,2,3,4,4], n_results: 5}})

  // test delete on collection
  // await chromaClient._delete({collectionName: "created-from-js", deleteEmbedding: {ids: ["test-id-1", "test-id-2"]}})
  // console.log("count after delete", await chromaClient.count({collectionName: "created-from-js"}))


  // manual index creation
  // peek collection-  this doesnt "work" because its not a "real" api endpoint, just a wrapper around the getCollection method
  // do a put on the collection
  // await chromaClient.put({collectionName: "created-from-js", putEmbedding: {embeddings: [1,2,3,4,5], ids: "test-id"}})


  // Test List Collections API
  let node;
  node = document.querySelector("#list-collections-result");
  node!.innerHTML = `<pre>${JSON.stringify(collections.data, null, 4)}</pre>`;

  node = document.querySelector("#collection-count");
  node!.innerHTML = `<pre>${count}</pre>`;

  node = document.querySelector("#collection-get");
  node!.innerHTML = `<pre>${JSON.stringify(getData, null, 4)}</pre>`;

  node = document.querySelector("#collection-query");
  node!.innerHTML = `<pre>${JSON.stringify(queryData, null, 4)}</pre>`;


};