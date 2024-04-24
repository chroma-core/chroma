import React, { useCallback, useEffect, useState } from "react";
import { ChromaClient } from "../../src/ChromaClient";
import { Collection } from "../../src/Collection";

const SAMPLE_DOCUMENTS = [
  "apple",
  "strawberry",
  "pineapple",
  "scooter",
  "car",
  "train",
];

const hashString = async (message: string) => {
  const encoder = new TextEncoder();
  const data = encoder.encode(message);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
};

const chroma = new ChromaClient({ path: "http://localhost:8000" });

const useCollection = () => {
  const [collection, setCollection] = useState<Collection>();

  useEffect(() => {
    chroma
      .getOrCreateCollection({ name: "demo-collection" })
      .then((collection) => setCollection(collection));
  }, []);

  return collection;
};

const useDocuments = (query?: string) => {
  const collection = useCollection();
  const [isLoading, setIsLoading] = useState(false);
  const [documents, setDocuments] = useState<
    { document: string; relativeDistance?: number }[]
  >([]);

  const revalidate = useCallback(async () => {
    setIsLoading(true);
    try {
      if (query) {
        collection?.query({ queryTexts: query }).then((results) => {
          const maxDistance = Math.max(...(results.distances?.[0] ?? []));
          setDocuments(
            results.documents[0].map((document, i) => {
              const distance = results.distances?.[0][i] ?? 0;
              const relativeDistance = distance / maxDistance;

              return {
                document: document!,
                relativeDistance,
              };
            }),
          );
        });
      } else {
        collection?.get({}).then((results) =>
          setDocuments(
            results.documents.map((document) => ({
              document: document!,
            })),
          ),
        );
      }
    } finally {
      setIsLoading(false);
    }
  }, [collection, query]);

  useEffect(() => {
    revalidate();
  }, [revalidate]);

  return { documents, revalidate, isLoading };
};

export function App() {
  const [query, setQuery] = useState("");
  const [isMutating, setIsMutating] = useState(false);
  const collection = useCollection();

  const trimmedQuery = query.trim();

  const { documents, revalidate, isLoading } = useDocuments(
    trimmedQuery === "" ? undefined : trimmedQuery,
  );

  const handleDocumentAdd = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();

    if (!collection) {
      return;
    }

    const currentTarget = event.currentTarget;

    const document = new FormData(currentTarget).get("document")!.toString();

    setIsMutating(true);
    try {
      await collection.upsert({
        ids: [await hashString(document)],
        documents: [document],
      });

      await revalidate();
      currentTarget.reset();
    } finally {
      setIsMutating(false);
    }
  };

  const handleLoadSampleData = async (event: React.FormEvent) => {
    event.preventDefault();

    if (!collection) {
      return;
    }

    setIsMutating(true);
    try {
      await collection.upsert({
        ids: await Promise.all(
          SAMPLE_DOCUMENTS.map(async (d) => hashString(d)),
        ),
        documents: SAMPLE_DOCUMENTS,
      });

      await revalidate();
    } finally {
      setIsMutating(false);
    }
  };

  return (
    <div>
      <h1>Chroma Browser Demo</h1>

      <p>
        This assumes that you have a locally running Chroma instance at port{" "}
        <code>8000</code> and that CORS is allowed for Parcel's dev server. To
        start Chroma, you can use this command:
      </p>

      <pre>
        CHROMA_SERVER_CORS_ALLOW_ORIGINS='["http://localhost:3000"]' chroma run
      </pre>

      <form onSubmit={handleDocumentAdd}>
        <h3>Add documents</h3>

        <textarea placeholder="foo" name="document" disabled={isMutating} />
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <button disabled={isMutating}>Create</button>
          <button
            disabled={isMutating}
            type="button"
            onClick={handleLoadSampleData}
          >
            Load sample data
          </button>
        </div>
      </form>

      <div>
        <h3>Query</h3>

        <p>
          Try loading the sample dataset, then search for "fruit" or "vehicle"
          and note how the results are ordered.
        </p>

        <label>Search</label>
        <input
          placeholder="fruit"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />

        {isLoading && <progress style={{ marginLeft: "1rem" }} />}

        <h4>Results</h4>

        <p>
          When you search for a query, the (xx) number is the distance of the
          document's vector from the vector of your query. A lower number means
          it's closer and thus more relevant to your query.
        </p>

        {documents.length > 0 || trimmedQuery !== "" ? (
          <label>
            {trimmedQuery === ""
              ? "(All documents)"
              : `Documents filtered by "${trimmedQuery}"`}
          </label>
        ) : (
          <label>(No documents have been created)</label>
        )}

        <ul>
          {documents.map(({ document, relativeDistance }) => (
            <li key={document}>
              {document}{" "}
              {relativeDistance
                ? `(${Math.round(relativeDistance * 100)})`
                : ""}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
