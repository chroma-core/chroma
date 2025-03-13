import React from "react";
import CliBlock from "@/components/markdoc/cli-block";
import Image from "next/image";
import { Database, GithubIcon, Scale, Triangle } from "lucide-react";
import TabbedCodeBlock from "@/components/markdoc/tabbed-code-block";
import Tab from "@/components/markdoc/tabs";
import TabbedUseCaseCodeBlock from "@/components/markdoc/tabbed-use-case-code-block";
import CodeBlock from "@/components/markdoc/code-block";
import Link from "next/link";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

const dataTable: {
  id: string;
  document: string;
  metadata: Record<string, string>;
}[] = [
  {
    id: "0701032e-c679-4187-8d10-2fb460affdac",
    document:
      " const model = new OpenAI({    temperature: 0, // increase temepreature to get more creative answers    modelName: 'gpt-3.5-turbo', //change this to gpt-4 if you have access  });  const chain = ConversationalRetrievalQAChain.fromLLM(    model,    vectorstore.asRetriever(),    {      qaTemplate: QA_PROMPT,      questionGeneratorTemplate: CONDENSE_PROMPT,      returnSourceDocuments: true, //The number of source documents returned is 4 by default    },  );  return chain;};",
    metadata: {
      type: "code",
    },
  },
  {
    id: "d80e47c5-68f2-4f2b-96d2-8594ae918038",
    document:
      "    Chroma provides a convenient wrapper around OpenAI's embedding API. This embedding function runs remotely on OpenAI's servers, and requires an API key. You can get an API key by signing up for an account at OpenAI.    The following OpenAI Embedding Models are supported:    text-embedding-ada-002    text-embedding-3-small    text-embedding-3-large    ",
    metadata: {
      type: "docs",
    },
  },
  {
    id: "86379e2e-23b2-4a20-9e4e-1d3151f071e1",
    document:
      '    import chromadb.utils.embedding_functions as embedding_functions    # This embedding function relies on the openai python package, which you can install with pip install openai    # You can pass in an optional model_name argument, which lets you choose which OpenAI embeddings model to use. By default, Chroma uses text-embedding-ada-002    openai_ef = embedding_functions.OpenAIEmbeddingFunction(                    api_key="YOUR_API_KEY",                    model_name="text-embedding-3-small"                )    ',
    metadata: {
      type: "code",
    },
  },
  {
    id: "ecd97981-6406-4277-ba70-09a2f157364c",
    document:
      '    import chromadb.utils.embedding_functions as embedding_functions    # To use the OpenAI embedding models on other platforms such as Azure, you can use the api_base and api_type parameters    openai_ef = embedding_functions.OpenAIEmbeddingFunction(                    api_key="YOUR_API_KEY",                    api_base="YOUR_API_BASE_PATH",                    api_type="azure",                    api_version="YOUR_API_VERSION",                    model_name="text-embedding-3-small"                )    ',
    metadata: {
      type: "code",
    },
  },
  {
    id: "379f94cf-2b8e-4d30-9dc4-14902f5d414f",
    document:
      "    Integrations    Embedding Integrations    Embeddings are the A.I-native way to represent any kind of data, making them the perfect fit for working with all kinds of A.I-powered tools and algorithms. They can represent text, images, and soon audio and video. There are many options for creating embeddings, whether locally using an installed library, or by calling an API.    Chroma provides lightweight wrappers around popular embedding providers, making it easy to use them in your apps. You can set an embedding function when you create a Chroma collection, which will be used automatically, or you can call them directly yourself.    Python\tTypescript    OpenAI\t✓\t✓    Google Gemini\t✓\t✓    Cohere\t✓\t✓    Hugging Face\t✓\t-    Instructor\t✓\t-    Hugging Face Embedding Server\t✓\t✓    Jina AI\t✓\t✓    Roboflow\t✓\t-    Ollama Embeddings\t✓\t✓    ",
    metadata: {
      type: "docs",
    },
  },
];

const goldenDataTable: {
  id: string;
  document: string;
  metadata: Record<string, string>;
}[] = [
  {
    id: "58d548a6-8093-48ba-ae58-b1b4d5f2a7a7",
    document: "use huggingface embedding with custom model",
    metadata: { "document-id": "c68fe44f-2b66-46e2-8b64-911fac842259" },
  },
  {
    id: "f44ab686-53aa-4044-937e-5e2246072069",
    document: "install and use instructor embedding package",
    metadata: { "document-id": "f5bee9fb-5139-4724-8123-93219a7b9160" },
  },
  {
    id: "f90742b9-3366-4d3e-adef-774a2b3eddea",
    document: "jina embedding function API key setup",
    metadata: { "document-id": "d88b7d40-9583-4e21-9fca-8b703efd09ec" },
  },
  {
    id: "2c0ebecc-bf8c-4ea2-87a0-601ae0d67c91",
    document: "optional model_name argument usage",
    metadata: { "document-id": "a9200285-2482-4f39-b5cd-bf2a3cc01afb" },
  },
  {
    id: "cc9479a1-3cd2-4757-a2cd-82f170067b38",
    document: "ollama embedding function usage",
    metadata: { "document-id": "8a2923bd-c774-4645-97e6-19ad6a2079c3" },
  },
];

const telemetryTable: {
  id: string;
  document: string;
  metadata: Record<string, string>;
}[] = [
  {
    id: "0d6b9dad-56df-4a98-9240-ac3b78b89fde",
    document:
      "how to create and use openai embedding function for my collection",
    metadata: {
      "chat-id": "a994c1c5-c1f0-4aeb-bd98-1d4c784908c8",
      timestamp: "2025-03-12T14:36:08.084753",
      role: "user",
    },
  },
  {
    id: "1886c50e-ce8c-405e-b395-8d6ce48415b5",
    document:
      'To create and use the OpenAI embedding function for your collection, you can follow these steps based on the samples provided in the Typescript and Python languages:### Using Typescript1. **Import the Required Module:**   Begin by importing the `OpenAIEmbeddingFunction` from the `chromadb` package.   ```typescript   import { OpenAIEmbeddingFunction } from \'chromadb\';   ```2. **Initialize the Embedding Function:**   Create an instance of the `OpenAIEmbeddingFunction` by passing your OpenAI API key and the model name you wish to use.   ```typescript   const embeddingFunction = new OpenAIEmbeddingFunction({       openai_api_key: "your_openai_api_key",       openai_model: "text-embedding-3-small"   });   ```3. **Generate Embeddings Directly:**   You can generate embeddings for your list of documents directly by calling the `generate` method.   ```typescript   const embeddings = embeddingFunction.generate(["document1", "document2"]);   ```4. **Using with a Collection:**   To use this embedding function within a collection, first create or get a collection from your client and pass the embedding function as a parameter.   ```typescript   let collection = await client.createCollection({       name: "your_collection_name",       embeddingFunction: embeddingFunction   });   collection = await client.getCollection({       name: "your_collection_name",       embeddingFunction: embeddingFunction   });   ```### Using Python1. **Install Required Package:**   Make sure to have the `openai` package installed.   ```bash   pip install openai   ```2. **Import the Required Module:**   Import the `OpenAIEmbeddingFunction` from the `chromadb.utils.embedding_functions`.   ```python   import chromadb.utils.embedding_functions as embedding_functions   ```3. **Initialize the Embedding Function:**   Create an instance of the `OpenAIEmbeddingFunction`, providing your OpenAI API key and the desired model name.   ```python   openai_ef = embedding_functions.OpenAIEmbeddingFunction(       api_key="your_openai_api_key",       model_name="text-embedding-3-small"   )   ```### Additional Details- **Flexibility in Deployment:**  The OpenAI embedding models can be used on other platforms like Azure by passing additional parameters like `api_base`, `api_type`, and `api_version`.- **Integration and Compatibility:**  Chroma provides lightweight wrappers for various embedding providers, ensuring usability across different A.I. tools and algorithms.4. **Use Your Embedding Function:**   After setting up your embedding function, you can easily integrate it into workflows or applications that require text embeddings, both by directly generating embeddings or embedding them within a collection.By following these detailed examples, you should be able to set up and use the OpenAI embedding function efficiently for your collection using the ChromaDB package in both Typescript and Python.',
    metadata: {
      "chat-id": "a994c1c5-c1f0-4aeb-bd98-1d4c784908c8",
      chunks:
        "['ee8e97ef-5d3c-4810-82d6-2aa4719b145c', 'ecd97981-6406-4277-ba70-09a2f157364c', '86379e2e-23b2-4a20-9e4e-1d3151f071e1', '379f94cf-2b8e-4d30-9dc4-14902f5d414f', 'd88b7d40-9583-4e21-9fca-8b703efd09ec']",
      timestamp: "2025-03-12T14:36:45.541134",
      role: "assistant",
    },
  },
  {
    id: "af6fdce4-d6b8-4655-9f68-7473ceafc5bd",
    document: "how to filter by metadata",
    metadata: {
      "chat-id": "f13d686a-bcfd-4dbb-9b80-c2d24360c34c",
      timestamp: "2025-03-12T14:38:48.355638",
      role: "user",
    },
  },
  {
    id: "8c5ded63-ec35-476c-b0de-45644992d762",
    document:
      'To filter by metadata, you can utilize the filtering capabilities that support various operators, as described in the provided information. Here’s a detailed breakdown of how you can achieve this:1. **Supported Operators for Filtering Metadata**:    - **$eq**: Checks if a metadata field is equal to a specified value. You can use this for strings, integers, and floats.   - **$ne**: Checks if a metadata field is not equal to a specified value.   - **$gt**: Checks if a metadata field is greater than a specified value. Applicable to integers and floats.   - **$gte**: Checks if a metadata field is greater than or equal to a specified value. Applicable to integers and floats.   - **$lt**: Checks if a metadata field is less than a specified value. Applicable to integers and floats.   - **$lte**: Checks if a metadata field is less than or equal to a specified value. Applicable to integers and floats.2. **Filtering Syntax Using the Where Filter**:   - To filter on metadata, you need to provide a "where" filter in the form of a dictionary. The dictionary structure should be as follows:     ```python     {         "metadata_field": {             <Operator>: <Value>         }     }     ```   - For example, if you want to filter where the "age" metadata is greater than 25, you would structure your query like this:     ```python     {         "age": {             "$gt": 25         }     }     ```3. **Combining Filters with Logical Operators**:   - **$and**: Use this operator to return results that satisfy all the conditions specified in the list.     ```python     {         "$and": [             {                 "age": {                     "$gt": 25                 }             },             {                 "salary": {                     "$gte": 50000                 }             }         ]     }     ```     The example above filters entries where "age" is greater than 25 and "salary" is at least 50,000.   - **$or**: Use this operator to return results that meet any of the specified conditions.     ```python     {         "$or": [             {                 "city": {                     "$eq": "New York"                 }             },             {                 "city": {                     "$eq": "Los Angeles"                 }             }         ]     }     ```     This example filters entries where the "city" is either "New York" or "Los Angeles".4. **Using $eq Operator**:   - The $eq operator is the default when using a string directly in the dictionary. For example:     ```python     {         "city": "New York"     }     ```     This is equivalent to:     ```python     {         "city": {             "$eq": "New York"         }     }     ```By structuring your queries to include these filters and operators, you can effectively filter through metadata to retrieve the desired results.',
    metadata: {
      "chat-id": "f13d686a-bcfd-4dbb-9b80-c2d24360c34c",
      chunks:
        "['2d3a276d-b299-4700-9831-a278e1a79c8d', 'ebc6a0c0-e751-43c9-9624-797cbf906924', '304b41dd-7b82-482f-bbf2-bbd73e27d4d1', '534d96d0-e83d-4184-9f46-f358242a94fb', '2650c9ec-5f7b-41b8-ab3c-638dc261a06f']",
      timestamp: "2025-03-12T14:39:06.602982",
      role: "assistant",
    },
  },
  {
    id: "012153d5-ecad-4e81-a4f3-1eceeba77fda",
    document: "How can I write an embedding function?",
    metadata: {
      "chat-id": "e6b5f0f0-8db0-46a6-a2ae-78040da1a137",
      timestamp: "2025-03-12T21:39:33.659Z",
      role: "user",
    },
  },
];

const SampleAppContent: React.FC = () => {
  return (
    <div className="flex flex-col w-full">
      <div className="flex items-center justify-between">
        <h1>Chat with your Docs</h1>
      </div>
      <CliBlock>chroma install chat_with_your_docs</CliBlock>
      <div>
        <Image
          src="/chat-with-your-docs.png"
          alt="chat-with-your-docs-preview"
          width={2331}
          height={509}
          priority
          className="rounded-lg border"
        />
      </div>
      <div className="flex justify-between items-stretch">
        <div className="w-[70%]">
          Create an AI-Chat application powered by your own data. This NextJS
          app leverages various retrieval techniques that enable you to query
          your documents with impressive accuracy. The implementation includes
          examples of chunking strategies, query expansion, metadata filtering,
          instruction retrieval approaches, and more. Every interaction with the
          application is logged in a dedicated "telemetry" collection on a
          Chroma DB, giving you visibility into your retrieval pipeline's
          performance. This makes it straightforward to spot anomalies, identify
          knowledge gaps, and continually refine your system based on real usage
          patterns.
        </div>
        <div className="flex flex-col -mt-2">
          <div>
            <p className="font-bold leading-[0px]">Technologies Used</p>
            <p className="leading-[0px]">NextJS</p>
          </div>
          <div>
            <p className="font-bold leading-[0px]">Publisher</p>
            <p className="leading-[0px]">Chroma</p>
          </div>
          <div>
            <p className="font-bold leading-[0px]">Date Published</p>
            <p className="leading-[0px]">03/12/2025</p>
          </div>
          <div className="flex items-center gap-2">
            <Scale className="w-5 h-5" />
            <p className="leading-[0px]">Apache-2.0 license</p>
          </div>
          <div>
            <p className="font-bold leading-[0px]">Languages</p>
            <div className="w-full h-1.5 rounded-full bg-blue-500" />
            <div className="flex items-center gap-2 mt-1">
              <div className="w-2 h-2 rounded-full bg-blue-500" />
              <p className="leading-[0px] text-sm">Typescript (100%)</p>
            </div>
          </div>
        </div>
      </div>
      <div>
        <h3>Getting Started</h3>
        <p>
          Install the Chroma CLI via our Python or NPM package, or using cURL
        </p>
        <TabbedUseCaseCodeBlock language="Terminal">
          <Tab label="Python">
            <CodeBlock
              content="pip install chromadb"
              language="python"
              showHeader={true}
              hideTicks
            />
          </Tab>
          <Tab label="Javascript">
            <CodeBlock
              content="npm install chromadb"
              language="python"
              showHeader={true}
              hideTicks
            />
          </Tab>
          <Tab label="cURL">
            <CodeBlock
              content="curl -sSL https://raw.githubusercontent.com/chroma-core/chroma/main/rust/cli/install/install.sh | bash"
              language="python"
              showHeader={true}
              hideTicks
            />
          </Tab>
        </TabbedUseCaseCodeBlock>
      </div>
      <p>Use the Chroma CLI to install the app and get its data.</p>
      <CodeBlock
        content="chroma install chat_with_your_docs"
        language="Terminal"
        showHeader={true}
        hideTicks
      />
      <p>Run the app locally!</p>
      <CodeBlock
        content="cd chat_with_your_docs && npm run dev"
        language="Terminal"
        showHeader={true}
        hideTicks
      />
      <h3>Code</h3>
      <Link
        href="https://github.com/chroma-core/chroma/tree/itai/demo-cli/examples/sample_apps/chat_with_your_docs"
        target="_blank"
        rel="noopener noreferrer"
      >
        <div className="flex items-center justify-between p-3 rounded-md border border-black">
          <div className="flex items-center gap-4">
            <Image
              src="/nextjs-icon.svg"
              alt="nextjs-icon"
              width={800}
              height={800}
              priority
              className="w-5 h-5 p-0 m-0"
            />
            <p className="leading-[0px]">NextJS app</p>
          </div>
          <GithubIcon className="w-5 h-5 p-0 m-0" />
        </div>
      </Link>
      <Link
        href="https://colab.research.google.com/drive/1Y-U6eGLKkrDyrnj8tfzTos4tIHyo_nhc?usp=sharing"
        target="_blank"
        rel="noopener noreferrer"
      >
        <div className="flex items-center justify-between p-3 rounded-md border border-black mt-2">
          <div className="flex items-center gap-4">
            <Image
              src="/jupyter_logo.svg.png"
              alt="jupyter-icon"
              width={800}
              height={800}
              priority
              className="w-5 h-5 p-0 m-0 grayscale"
            />
            <p className="leading-[0px]">
              Jupyter Notebook: Generate a golden dataset
            </p>
          </div>
          <Image
            src="/colab-icon.png"
            alt="colab-icon"
            width={225}
            height={225}
            priority
            className="w-7 h-7 p-0 m-0 grayscale"
          />
        </div>
      </Link>
      <h3>Data</h3>
      <Accordion type="multiple" className="p-0">
        <AccordionItem value="item-1" className="border-none m-0 p-0  -mt-10">
          <AccordionTrigger className="p-3 rounded-md border border-black">
            <div className="flex items-center gap-4 justify-between w-full">
              <div className="flex items-center gap-4">
                <Database className="w-4 h-4" />
                <p className="leading-[0px] text-base">
                  Chroma Docs and Code Examples
                </p>
              </div>
              <p className="leading-[0px] font-mono mr-2">221 Records</p>
            </div>
          </AccordionTrigger>
          <AccordionContent>
            <Table className="w-full table-fixed">
              <TableHeader className="bg-gray-100">
                <TableRow className="border-b-0!">
                  <TableHead
                    key="id-header"
                    className="h-8 w-[20%] py-1 text-black"
                  >
                    ID
                  </TableHead>
                  <TableHead
                    key="doc-header"
                    className="h-8 w-auto py-1 text-black"
                  >
                    Document
                  </TableHead>
                  <TableHead
                    key="metadata-header"
                    className="h-8 w-[20%] py-1 text-black"
                  >
                    Metadata
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {dataTable.map((row, i) => (
                  <TableRow
                    key={i}
                    className="border-b-0 hover:bg-gray-100 cursor-pointer relative"
                  >
                    <TableCell
                      key={`id-${i}`}
                      className="py-0 h-8 truncate w-[20%]"
                    >
                      {row.id}
                    </TableCell>
                    <TableCell key={`doc-${i}`} className="py-0 h-8 truncate">
                      {row.document}
                    </TableCell>
                    <TableCell
                      key={`metadata-${i}`}
                      className="py-0 h-8 truncate w-[20%]"
                    >
                      {Object.keys(row.metadata).map((key) => (
                        <Badge
                          key={key}
                          className="inline-block rounded bg-gray-200 text-gray-900 font-medium text-[13px] px-1.5 py-0 leading-5 pointer-events-none truncate max-w-full"
                        >
                          {key}:{row.metadata[key]}
                        </Badge>
                      ))}
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </AccordionContent>
        </AccordionItem>
        <AccordionItem value="item-2" className="border-none -mt-5">
          <AccordionTrigger className="p-3 rounded-md border border-black">
            <div className="flex items-center gap-4 justify-between w-full">
              <div className="flex items-center gap-4">
                <Database className="w-4 h-4" />
                <p className="leading-[0px] text-base">Telemetry</p>
              </div>
              <p className="leading-[0px] font-mono mr-2">89 Records</p>
            </div>
          </AccordionTrigger>
          <AccordionContent>
            <Table className="w-full table-fixed">
              <TableHeader className="bg-gray-100">
                <TableRow className="border-b-0!">
                  <TableHead
                    key="id-header"
                    className="h-8 w-[20%] py-1 text-black"
                  >
                    ID
                  </TableHead>
                  <TableHead
                    key="doc-header"
                    className="h-8 w-auto py-1 text-black"
                  >
                    Document
                  </TableHead>
                  <TableHead
                    key="metadata-header"
                    className="h-8 w-[20%] py-1 text-black"
                  >
                    Metadata
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {telemetryTable.map((row, i) => (
                  <TableRow
                    key={i}
                    className="border-b-0 hover:bg-gray-100 cursor-pointer relative"
                  >
                    <TableCell
                      key={`id-${i}`}
                      className="py-0 h-8 truncate w-[20%]"
                    >
                      {row.id}
                    </TableCell>
                    <TableCell key={`doc-${i}`} className="py-0 h-8 truncate">
                      {row.document}
                    </TableCell>
                    <TableCell
                      key={`metadata-${i}`}
                      className="py-0 h-8 w-[20%]"
                    >
                      <Badge
                        key={"role"}
                        className="inline-block rounded bg-gray-200 text-gray-900 font-medium text-[13px] px-1.5 py-0 leading-5 pointer-events-none truncate max-w-full"
                      >
                        {`role: ${row.metadata["role"]}`}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </AccordionContent>
        </AccordionItem>
        <AccordionItem value="item-3" className="border-none -mt-5">
          <AccordionTrigger className="p-3 rounded-md border border-black">
            <div className="flex items-center gap-4 justify-between w-full">
              <div className="flex items-center gap-4">
                <Database className="w-4 h-4" />
                <p className="leading-[0px] text-base">Golden Dataset</p>
              </div>
              <p className="leading-[0px] font-mono mr-2">221 Records</p>
            </div>
          </AccordionTrigger>
          <AccordionContent>
            <Table className="w-full table-fixed">
              <TableHeader className="bg-gray-100">
                <TableRow className="border-b-0!">
                  <TableHead
                    key="id-header"
                    className="h-8 w-[20%] py-1 text-black"
                  >
                    ID
                  </TableHead>
                  <TableHead
                    key="doc-header"
                    className="h-8 w-auto py-1 text-black"
                  >
                    Document
                  </TableHead>
                  <TableHead
                    key="metadata-header"
                    className="h-8 w-[20%] py-1 text-black"
                  >
                    Metadata
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {goldenDataTable.map((row, i) => (
                  <TableRow
                    key={i}
                    className="border-b-0 hover:bg-gray-100 cursor-pointer relative"
                  >
                    <TableCell
                      key={`id-${i}`}
                      className="py-0 h-8 truncate w-[20%]"
                    >
                      {row.id}
                    </TableCell>
                    <TableCell key={`doc-${i}`} className="py-0 h-8 truncate">
                      {row.document}
                    </TableCell>
                    <TableCell
                      key={`metadata-${i}`}
                      className="py-0 h-8 w-[20%]"
                    >
                      <Badge
                        key={"role"}
                        className="inline-block rounded bg-gray-200 text-gray-900 font-medium text-[13px] px-1.5 py-0 leading-5 pointer-events-none truncate max-w-full"
                      >
                        {`document-id: ${row.metadata["document-id"]}`}
                      </Badge>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </div>
  );
};

export default SampleAppContent;
