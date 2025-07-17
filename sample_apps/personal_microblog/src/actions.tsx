"use server";

import { EnrichedTweetModel, NewPostResponseTweetModel, PartialAssistantPost, TweetModelBase, TweetStatus, UserWithStreamingAIResponseTweetModel } from "@/types";
import { openai } from '@ai-sdk/openai';
import { createStreamableValue, StreamableValue } from 'ai/rsc';
import { generateText, generateObject, jsonSchema, streamText } from 'ai';
import {
  addPostModelToChromaCollection,
  chromaGetResultsToPostModels,
  chromaQueryResultsToPostModels,
  generateId,
  getReferencedPostsIds,
  unixTimestampNow,
} from "./util";


import { ChromaClient } from "chromadb";
import { OpenAIEmbeddingFunction } from "@chroma-core/openai";

const CHROMA_HOST = process.env.CHROMA_HOST;
const CHROMA_CLOUD_API_KEY = process.env.CHROMA_CLOUD_API_KEY;
const CHROMA_TENANT = process.env.CHROMA_TENANT;
const CHROMA_DB = process.env.CHROMA_DB;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const usingChromaLocal = CHROMA_HOST === "localhost";

if (CHROMA_HOST == null) {
  throw new Error("CHROMA_HOST is not set");
}
if (CHROMA_HOST == 'api.trychroma.com' && CHROMA_CLOUD_API_KEY == null) {
  throw new Error("CHROMA_CLOUD_API_KEY is not set");
}
if (CHROMA_TENANT == null) {
  throw new Error("CHROMA_TENANT is not set");
}
if (CHROMA_DB == null) {
  throw new Error("CHROMA_DB is not set");
}
if (OPENAI_API_KEY == null) {
  throw new Error("OPENAI_API_KEY is not set");
}

const chromaClient = new ChromaClient({
  ssl: !usingChromaLocal,
  host: CHROMA_HOST,
  database: CHROMA_DB,
  tenant: CHROMA_TENANT,
  headers: {
    "x-chroma-token": CHROMA_CLOUD_API_KEY ?? "",
  },
});

const chromaCollection = await chromaClient.getOrCreateCollection({
  name: "personal-microblog-tweets",
  embeddingFunction: new OpenAIEmbeddingFunction({
    apiKey: OPENAI_API_KEY ?? "",
    modelName: "text-embedding-3-small",
  }),
});

const llmModel = openai('gpt-4-turbo')

/**
 * Returns the posts from `cursor` to `cursor - pageSize`, inclusive
 * The new cursor is `cursor - pageSize - 1`
 */
export async function getPosts(cursor?: number): Promise<{ posts: EnrichedTweetModel[], cursor: number }> {
  if (cursor != undefined && cursor <= -1) {
    return {
      posts: [],
      cursor: -1,
    };
  }
  const pageSize = 25;
  let basePosts: TweetModelBase[] = [];
  let newCursor: number = -1;
  if (cursor != undefined) {
    let start = cursor - pageSize + 1;
    if (start < 0) {
      start = 0;
    }
    const chromaResult = await chromaCollection.get({
      where: { "role": "user" },
      include: ["documents", "metadatas"],
      limit: cursor - start + 1,
      offset: start,
    });
    basePosts = chromaGetResultsToPostModels(chromaResult).reverse();
    newCursor = start - 1;
  } else {
    const chromaResult = await chromaCollection.get({
      where: { "role": "user" },
      include: ["documents", "metadatas"],
    });
    const postModels = chromaGetResultsToPostModels(chromaResult);
    const count = postModels.length;
    let start = count - pageSize + 1;
    if (start < 0) {
      start = 0;
    }
    basePosts = postModels.slice(start).reverse();
    newCursor = start - 1;
  }
  const enrichedPosts = await enrichPosts(basePosts);
  return {
    posts: enrichedPosts,
    cursor: newCursor,
  };
}

export async function getPostById(id: string): Promise<TweetModelBase | undefined> {
  const post = await chromaCollection.get({
    ids: [id],
  });
  const res = chromaGetResultsToPostModels(post)
  return res.length > 0 ? res[0] : undefined;
}

async function getPostsByIds(ids: string[]): Promise<TweetModelBase[]> {
  const posts = await chromaCollection.get({
    ids: ids,
  });
  return chromaGetResultsToPostModels(posts);
}

export async function getPostReplies(id: string): Promise<EnrichedTweetModel[]> {
  const posts = await chromaCollection.get({
    where: {
      "threadParentId": id,
    }
  });
  return await enrichPosts(chromaGetResultsToPostModels(posts));
}

async function enrichPosts(posts: TweetModelBase[]): Promise<EnrichedTweetModel[]> {
  let referencedPostsIds: string[] = posts.flatMap(getReferencedPostsIds);
  if (referencedPostsIds.length == 0) {
    return posts.map((post) => ({
      ...post,
      type: 'enriched',
      enrichedCitations: [],
    }));
  }
  referencedPostsIds = Array.from(new Set(referencedPostsIds));
  const referencedPosts = await getPostsByIds(referencedPostsIds);
  const referencedPostsMap = new Map(referencedPosts.map((p) => [p.id, p]));
  const enrichedPosts: EnrichedTweetModel[] = posts.map((post) => {
    return {
      ...post,
      type: 'enriched',
      enrichedAiReply: post.aiReplyId ? referencedPostsMap.get(post.aiReplyId) : undefined,
      enrichedCitations: post.citations.map((citationId) => referencedPostsMap.get(citationId)).filter((p) => p != undefined),
    };
  });
  return enrichedPosts;
}

export async function semanticSearch(query: string): Promise<EnrichedTweetModel[]> {
  const chromaResult = await chromaCollection.query({
    queryTexts: [query],
    nResults: 5,
    where: {"role": "user"},
  });
  const posts = chromaQueryResultsToPostModels(chromaResult);
  return await enrichPosts(posts);
}

export async function fullTextSearch(query: string): Promise<EnrichedTweetModel[]> {
  const chromaResult = await chromaCollection.query({
    queryTexts: [query],
    nResults: 10,
    whereDocument: {
      "$contains": query
    }
  });
  const posts = chromaQueryResultsToPostModels(chromaResult);
  return await enrichPosts(posts);
}

export async function publishNewUserPost(rawBody: string, threadParentId?: string): Promise<NewPostResponseTweetModel> {
  const { citationIds, newBody } = extractTweetCitations(rawBody);
  const newPost: TweetModelBase = {
    type: 'base',
    id: generateId(),
    threadParentId: threadParentId,
    role: "user",
    body: newBody,
    citations: citationIds,
    date: unixTimestampNow(),
    status: "done",
  };
  let partialAssistantPost: PartialAssistantPost | undefined = undefined;
  if (newPost.body.includes("@assistant")) {
    partialAssistantPost = getAssistantResponse(newPost, newPost.id);
    newPost.aiReplyId = partialAssistantPost?.id;
  }
  addPostModelToChromaCollection(newPost, chromaCollection).catch(console.error);
  return partialAssistantPost ? {
    ...newPost,
    type: 'streaming',
    aiReply: partialAssistantPost,
  } : newPost;
}

function extractTweetCitations(body: string): { citationIds: string[], newBody: string } {
  const citationIds: string[] = [];
  const newBody = body.replace(/\[\[([a-zA-Z0-9]+)\]\]/g, (_, p1) => {
    citationIds.push(p1);
    return ``;
  });
  const uniqueCitationIds = [...new Set(citationIds)];
  return { citationIds: uniqueCitationIds, newBody };
}

function getAssistantResponse(userInput: TweetModelBase, parentThreadId: string): PartialAssistantPost {
  const id = generateId();
  const stream = createStreamableValue<string, any>();
  const assistantPost: PartialAssistantPost = {
    type: 'base',
    id,
    threadParentId: parentThreadId,
    role: "assistant",
    body: "Thinking...",
    citations: [],
    date: unixTimestampNow(),
    status: "processing",
    stream: stream.value,
  };
  addPostModelToChromaCollection(assistantPost, chromaCollection).catch(console.error);
  processAssistantResponse(userInput, assistantPost, stream).catch(console.error); // Run in background
  return assistantPost;
}

/*
 * All of this could easily be made concurrent, but I negelected to do that so the state stream
 * looks cooler.
 */
async function processAssistantResponse(userPost: TweetModelBase, assistantReponsePost: PartialAssistantPost, stream: any) {
  const cleanedUserInput = userPost.body.replace(/@assistant|\[\[[a-zA-Z0-9]+\]\]/g, "").trim();

  try {
    stream.update("--BEGIN--");
    stream.update("Doing vector search for related posts...");
    const semanticContext = await chromaCollection.query({
      queryTexts: [cleanedUserInput],
      nResults: 10,
      where: {
        "role": "user",
      }
    });
    stream.update("Generating time range query...");
    const queryRange = await generateQueryRange(cleanedUserInput);
    let context = chromaQueryResultsToPostModels(semanticContext);

    if (queryRange.start != null && queryRange.end != null) {
      const temporalContext = await chromaCollection.query({
        queryTexts: [cleanedUserInput],
        nResults: 10,
        where: {
          "$and": [
            {
              "date": {
                "$gte": queryRange.start as number,
              },
            },
            {
              "date": {
                "$lte": queryRange.end as number,
              },
            }
          ],
        },
      });
      context.push(...chromaQueryResultsToPostModels(temporalContext));
    }

    stream.update("Generating full text search terms...");
    const searchTerms = await generateFullTextSearchTerms(cleanedUserInput);

    if (searchTerms.length > 0) {
      stream.update("Performing full text search...");
      const ftsFilter = searchTerms.length > 1
        ? { "$or": searchTerms.map(term => ({ "$contains": term })) }
        : { "$contains": searchTerms[0] };
      const fullTextSearchContext = await chromaCollection.query({
        queryTexts: [cleanedUserInput],
        nResults: 10,
        whereDocument: ftsFilter,
      });
      context.push(...chromaQueryResultsToPostModels(fullTextSearchContext));
    }

    const citationIds = context.map((p) => p.id).filter((id) => id != assistantReponsePost.id && id != userPost.id);
    const citationIdsSet = Array.from(new Set(citationIds));
    const citationIdToPost = new Map(context.map((p) => [p.id, p]));
    const filteredContext = citationIdsSet.map((id) => citationIdToPost.get(id)).filter((p) => p != undefined);


    stream.update("Reranking context...");
    const rerankedContext = await rerank(cleanedUserInput, filteredContext);

    // Only take the top 5 citations in order to avoid Metadata quota limits
    const topCitations = rerankedContext.slice(0, 5);
    assistantReponsePost.citations = topCitations.map((p) => p.id);

    const formattedContext = topCitations.map((post, i) => {
      return `(${i + 1}) ${new Date(post.date * 1000).toISOString()}: ${post.body}`;
    }).join("\n\n");

    stream.update("--CITATIONS--");

    for (const citation of assistantReponsePost.citations) {
      stream.update(citation);
    }


    const { textStream } = await streamText({
      model: llmModel,
      messages: [
        {
          role: 'system',
          content: `You are an AI assistant that helps users with their questions.

          Context from previous posts:
          ${formattedContext}

          Instructions:
          1. Keep your responses short and to the point.
          2. Use markdown for formatting and don't use emojis.
          3. Base your response on the context provided when relevant.
          4. If the context does not provide enough information, tell the user that you don't know.
          5. Reference any context you use inline with [^1], [^2], etc.

          Example:
          (1) 2025-06-01 00:00:00: I went to the gym.
          (2) 2025-06-02 00:00:00: I went to the grocery store.
          User: "What did I do last week?"
          Response: You went to the gym[^1] and the grocery store[^2] last week.`
        },
        {
          role: 'user',
          content: cleanedUserInput
        }
      ],
      maxTokens: 240,
    });

    stream.update("--BODY--");
    let text = "";
    for await (const textPart of textStream) {
      text += textPart;
      stream.update(text);
    }

    assistantReponsePost.body = text;
    assistantReponsePost.status = "done";
  } catch (error) {
    console.error('Error generating assistant response:', error);
    assistantReponsePost.body = "Sorry, I encountered an error while processing your request.";
    assistantReponsePost.status = "error";
    stream.update("--ERROR--");
  } finally {
    stream.done();
  }

  addPostModelToChromaCollection(assistantReponsePost, chromaCollection).catch(console.error);
}

interface QueryRange {
  /* UNIX timestamp */
  start?: number;
  end?: number;
}

async function generateQueryRange(userInput: string): Promise<QueryRange> {
  const { object } = await generateObject({
    model: llmModel,
    schema: jsonSchema<QueryRange>({
      type: "object",
      properties: {
        start: { type: "string", format: "date-time" },
        end: { type: "string", format: "date-time" },
      },
    }),
    messages: [
      {
        role: 'system',
        content: `
        You will determine the appropriate time range that should be searched based on the user's query.
        The current date is ${new Date().toISOString()}.
        The user's query is: ${userInput}
        The time range should be in the format of this json:
        {
          "start": "YYYY-MM-DD HH:MM:SS",
          "end": "YYYY-MM-DD HH:MM:SS"
        }
        If the user's query is not related to time, return {}.

        Examples:
        Current date: 2025-06-01
        User: "What did I do last week?"
        Response: {
          "start": "2025-05-25 00:00:00",
          "end": "2025-06-01 00:00:00"
        }
        User: "What did I do last month?"
        Response: {
          "start": "2025-05-01 00:00:00",
          "end": "2025-06-01 00:00:00"
        }
        User: "How do I make cookies?"
        Response: {}
        `
      },
      {
        role: 'user',
        content: userInput
      }
    ],
  });
  return {
    start: object.start ? new Date(object.start).getTime() / 1000 : undefined,
    end: object.end ? new Date(object.end).getTime() / 1000 : undefined,
  };
}

async function generateFullTextSearchTerms(userInput: string): Promise<string[]> {
  const { object } = await generateObject({
    model: llmModel,
    output: "array",
    schema: jsonSchema<string>({
      type: "string",
    }),
    messages: [
      {
        role: 'system',
        content: `
        You are a helpful assistant that generates specific search terms for full-text search based on a user's query.
        Generate 3-5 key terms or phrases that would help find relevant posts in a personal microblog.
        Focus on:
        - Key nouns and important concepts
        - Specific activities or topics mentioned
        - Important keywords that would appear in relevant posts

        Return an array of search terms as strings.

        Examples:
        User: "What did I learn about machine learning last month?"
        Response: ["machine learning", "ML"]

        User: "Show me posts about cooking dinner"
        Response: ["cooking", "dinner", "recipe", "food", "kitchen"]

        User: "What meetings did I have?"
        Response: ["meeting", "call", "conference", "discussion", "scheduled"]
        `
      },
      {
        role: 'user',
        content: userInput
      }
    ],
  });
  return object;
}

async function rerank(userInput: string, posts: TweetModelBase[]): Promise<TweetModelBase[]> {
  const { object } = await generateObject({
    model: llmModel,
    output: "array",
    schema: jsonSchema<number>({
      type: "number",
    }),
    messages: [
      {
        role: 'system',
        content: `
        You are a helpful assistant that reranks a list of posts based on the user's query.
        Remove any posts that are not relevant to the user's query or not necessary to answer the user's query.
        Reorder them based on how relevant they are to the user's query.
        The user's query is: ${userInput}
        The posts are: ${posts.map((p, i) => `(${i}) ${p.body}`).join("\n")}`
      }
    ]
  });
  return object.map((i) => posts[i]).filter((p) => p != undefined);
}
