---
title: HoneyHive
---

[HoneyHive](https://www.honeyhive.ai/) is the end-to-end AI observability and evaluation platform for building reliable AI agents that work.

HoneyHive offers libraries in both Typescript and Python for executing and recording evaluations, and it seamlessly integrates with Chroma.

Follow the [HoneyHive Installation Guide](https://docs.honeyhive.ai/tracing/integrations/integration-prereqs) to get your API key and initialize the tracer. Ensure that you have the OpenAI API key set in your environment variables.

Next, start the Chroma server (`chroma run --path ./getting-started`) and paste the following code into your evaluation script to trace the RAG pipeline.

Example evaluation script in JavaScript: (for the full implementation, see our [integrations page](https://docs.honeyhive.ai/integrations/chromadb#example))

```jsx
import { ChromaClient, OpenAIEmbeddingFunction } from "chromadb";
import OpenAI from "openai";

import { HoneyHiveTracer } from "honeyhive";

const tracer = await HoneyHiveTracer.init({
    apiKey: "MY_HONEYHIVE_API_KEY",
    project: "MY_PROJECT_NAME",
    sessionName: "chroma",
});

const openai_client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
const client = new ChromaClient();

const embeddingFunction = new OpenAIEmbeddingFunction({
    openai_api_key: process.env.OPENAI_API_KEY ?? "",
});

const collection = client.getOrCreateCollection({
    name: "scifact",
    embeddingFunction,
});

const article = {
    doc_id: 1,
    title:
        "ALDH1 is a marker of normal and malignant human mammary stem cells and a predictor of poor clinical outcome.",
    abstract: [
        "Application of stem cell biology to breast cancer research has been limited by the lack of simple methods for identification and isolation of normal and malignant stem cells.",
        "Utilizing in vitro and in vivo experimental systems, we show that normal and cancer human mammary epithelial cells with increased aldehyde dehydrogenase activity (ALDH) have stem/progenitor properties.",
        "These cells contain the subpopulation of normal breast epithelium with the broadest lineage differentiation potential and greatest growth capacity in a xenotransplant model.",
        "In breast carcinomas, high ALDH activity identifies the tumorigenic cell fraction, capable of self-renewal and of generating tumors that recapitulate the heterogeneity of the parental tumor.",
        "In a series of 577 breast carcinomas, expression of ALDH1 detected by immunostaining correlated with poor prognosis.",
        "These findings offer an important new tool for the study of normal and malignant breast stem cells and facilitate the clinical application of stem cell concepts.",
    ],
    structured: false,
};

async function processDocument() {
    (await collection).add({
        ids: article["doc_id"].toString(),
        documents: `${article["title"]}. ${article["abstract"].join(" ")}`,
        metadatas: { structured: article["structured"] },
    });
}

await processDocument();

const buildPromptWithContext = (claim, context) => [
    {
        role: "system",
        content:
            "I will ask you to assess whether a particular scientific claim, based on evidence provided. " +
            "Output only the text 'True' if the claim is true, 'False' if the claim is false, or 'NEE' if there's " +
            "not enough evidence.",
    },
    {
        role: "user",
        content: `
            The evidence is the following:

            ${context.join(" ")}

            Assess the following claim on the basis of the evidence. Output only the text 'True' if the claim is true,
            'False' if the claim is false, or 'NEE' if there's not enough evidence. Do not output any other text.

            Claim:
            ${claim}

            Assessment:
        `,
    },
];

async function assessClaims(claims) {
    const claimQueryResult = await (
        await collection
    ).query({
        queryTexts: claims,
        include: ["documents", "distances"],
        nResults: 3,
    });
    const responses = [];

    for (let i = 0; i < claimQueryResult.documents.length; i++) {
        const claim = claims[i];
        const context = claimQueryResult.documents[i];
        if (context.length === 0) {
            responses.push("NEE");
            continue;
        }

        const response = await openai_client.chat.completions.create({
            model: "gpt-4o-mini",
            messages: buildPromptWithContext(claim, context),
            max_tokens: 3,
        });

        const formattedResponse = response.choices[0].message.content?.replace(
            "., ",
            "",
        );
        console.log("Claim: ", claim);
        console.log("Response: ", formattedResponse);
        responses.push(formattedResponse);
    }

    return responses;
}

const tracedAssessClaims = tracer.traceFunction()(assessClaims);

const tracedMain = async () => {
    await tracedAssessClaims([
        "ALDH1 expression is associated with better breast cancer outcomes.",
        "ALDH1 expression is associated with poorer prognosis in breast cancer.",
    ]);
};
await tracedMain();
```

To learn more about HoneyHive, visit the
[official documentation](https://docs.honeyhive.ai/introduction/what-is-hhai).
