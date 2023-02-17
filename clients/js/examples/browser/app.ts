// import env.ts
import chromaClient from "chromajs"

window.onload = async () => {

  // create a new ChromaClient class
  // throw new Error("1");

  // create a new chroma client
  const chroma = chromaClient("http://localhost:8000");
  await chroma.reset()

  const collection = await chroma.createCollection("test-from-js");
  let count = await collection.count();
  
  for (let i = 0; i < 20; i++) {
    await collection.add(
      [1, 2, 3, 4, 5],
      "test-id-" + i.toString()
    )
  }

  const queryData = await collection.query([1, 2, 3, 4, 5], 5);
  
  console.log("queryData", queryData);

  // const collections = await chroma.listCollections();
  // const collection2 = await chroma.getCollection("test-from-js");
  // const getData = await collection.get("test-id-1");


  // TODO: come back to this one
  // await chroma.deleteCollection("test-from-js");

  // Test List Collections API
  // let node;
  // node = document.querySelector("#list-collections-result");
  // node!.innerHTML = `<pre>${JSON.stringify(collections.data, null, 4)}</pre>`;

  // node = document.querySelector("#collection-count");
  // node!.innerHTML = `<pre>${count}</pre>`;

  // node = document.querySelector("#collection-get");
  // node!.innerHTML = `<pre>${JSON.stringify(getData, null, 4)}</pre>`;

  // node = document.querySelector("#collection-query");
  // node!.innerHTML = `<pre>${JSON.stringify(queryData, null, 4)}</pre>`;


};