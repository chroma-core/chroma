import { ChromaClient } from '../../src/ChromaClient';
// import env.ts

window.onload = async () => {
  const chroma = new ChromaClient({ path: "http://localhost:8000" });
  await chroma.reset();

  const collection = await chroma.createCollection({ name: "test-from-js" });
  console.log("collection", collection);

  // first generate some data
  var ids: string[] = [];
  var embeddings: Array<any> = [];
  var metadatas: Array<any> = [];
  for (let i = 0; i < 100; i++) {
    ids.push("test-id-" + i.toString());
    embeddings.push([1, 2, 3, 4, 5]);
    metadatas.push({ test: "test" });
  }

  let add = await collection.add({ ids, embeddings, metadatas });
  console.log("add", add);

  let count = await collection.count();
  console.log("count", count);

  const queryData = await collection.query({
    queryEmbeddings: [1, 2, 3, 4, 5],
    nResults: 5,
    where: { test: "test" }
  });

  console.log("queryData", queryData);

  await collection.delete();

  let count2 = await collection.count();
  console.log("count2", count2);

  const collections = await chroma.listCollections();
  console.log("collections", collections);

  // this code is commented out so that it is easy to see the output on the page if desired
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
