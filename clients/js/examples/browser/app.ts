// import env.ts
import chromaClient from "chromadb"

window.onload = async () => {
  const chroma = chromaClient("http://localhost:8000");
  await chroma.reset()

  const collection = await chroma.createCollection("test-from-js");
  
  for (let i = 0; i < 20; i++) {
    await collection.add(
      "test-id-" + i.toString(),
      [1, 2, 3, 4, 5],
      { "test": "test" }
    )
  }

  let count = await collection.count();
  console.log("count", count);

  const queryData = await collection.query([1, 2, 3, 4, 5], 5, { "test": "test" });
  
  console.log("queryData", queryData);

  await collection.delete()

  let count2 = await collection.count();
  console.log("count2", count2);

  const collections = await chroma.listCollections();
  console.log("collections", collections);

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