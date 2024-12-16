import React from "react";
import CodeBlock from "@/components/markdoc/code-block";

const notFoundCode = `
import chromadb

client = chromadb.Client()
collection = client.get_collection(name="chroma_docs")
results = collection.get(ids=["page"])["documents"]
print(results) # Not found []
`

const NotFound = () => {
    return <div className="flex items-center justify-center w-full h-full">
        <CodeBlock className="p-4 bg-gray-800 text-white" content={notFoundCode} language="python" showHeader={true}/>
    </div>
}

export default NotFound;