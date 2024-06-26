import { IEmbeddingFunction } from "./embeddings/IEmbeddingFunction";
import {
  BaseChromaDoc,
  ChromaDoc,
  DocQuery,
  EmbeddingDoc,
  Metadata,
  QueryDoc,
} from "./types";
export async function computeEmbeddings<T extends BaseChromaDoc>(
  reqParams: AddDocumentsParams,
  embeddingFunction: IEmbeddingFunction
): Promise<AddDocumentsParams> {
  const docsWithoutContentsOrEmbeddings = documents.filter(
    (doc) => !doc.contents && !doc.embedding
  );

  if (docsWithoutContentsOrEmbeddings.length > 0) {
    throw new Error(
      "The following documents have neither contents nor embeddings: " +
        docsWithoutContentsOrEmbeddings.map((doc) => doc.id).join(", ")
    );
  }

  const docsMissingEmbeddings = documents.filter(
    (doc) => !doc.embedding
  ) as (ChromaDoc & { contents: string })[];

  if (docsMissingEmbeddings.length === 0) {
    return documents as (T & EmbeddingDoc)[];
  }
  const embeddings = await embeddingFunction.generate(
    docsMissingEmbeddings.map((doc) => doc.contents)
  );

  docsMissingEmbeddings.map((doc, index) => {
    doc.embedding = embeddings[index];
  });

  return documents as (T & EmbeddingDoc)[];
}
