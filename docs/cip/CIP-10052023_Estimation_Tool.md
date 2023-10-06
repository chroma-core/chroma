# CIP-10052023: Sizing Estimation Tool

## Status

Current Status: `Draft`

## **Motivation**

On daily basis people are either asking or being affected by lack of coherent scheme for estimating the sizing of a Chroma deployment. This CIP proposes a method for estimating the hardware size required for running Chroma and a tool implementing the method.

## **Public Interfaces**

We propose the introduction of a new CLI group of commands called `estimate` which will contain the following sub-commands:

- `baseline` - Gives the user a print-out table of the baseline hardware increments of both CPU and Memory and number of vectors that can be stored broken down by common vector dimensionalities. An example output is shown below:

```bash
                                     Estimated Max Vectors                                     
┏━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
┃ Memory (GB) ┃ vCPU ┃ 1536 Dimensions  ┃ 768 Dimensions  ┃ 512 Dimensions  ┃ 368 Dimensions  ┃
┡━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
│ 1           │ 1    │ 122.3 thousand   │ 244.7 thousand  │ 367.0 thousand  │ 510.6 thousand  │
│ 2           │ 1    │ 244.7 thousand   │ 489.3 thousand  │ 734.0 thousand  │ 1.0 million     │
│ 4           │ 2    │ 489.3 thousand   │ 978.7 thousand  │ 1.5 million     │ 2.0 million     │
│ 8           │ 4    │ 978.7 thousand   │ 2.0 million     │ 2.9 million     │ 4.1 million     │
│ 16          │ 8    │ 2.0 million      │ 3.9 million     │ 5.9 million     │ 8.2 million     │
│ 32          │ 16   │ 3.9 million      │ 7.8 million     │ 11.7 million    │ 16.3 million    │
│ 64          │ 32   │ 7.8 million      │ 15.7 million    │ 23.5 million    │ 32.7 million    │
│ 128         │ 64   │ 15.7 million     │ 31.3 million    │ 47.0 million    │ 65.4 million    │
└─────────────┴──────┴──────────────────┴─────────────────┴─────────────────┴─────────────────┘
Note: 30% memory overhead for Chroma server and OS
```

- `estimate` - This command goes through a series of questions (inputs can be added as params in case of an automated system execution) and produces an estimate of the sizing required to host a Chroma server with extrapolated number of vectors. The command should be capable of providing `now` estimate as well as over specified user increments with anticipated growth over those increments. An example output is shown below:

```bash
                                        Estimated Memory                                         
┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
┃ Timeline  ┃ Documents/Vectors (Chroma) ┃ Memory (GB) ┃ vCPU ┃ Dimensions ┃ Embedding Function ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
│ Now       │ 29.3 thousand              │ 4           │ 2    │ 1536       │ OpenAI             │
│ 6 months  │ 1.1 million                │ 16          │ 8    │ 1536       │ OpenAI             │
│ 12 months │ 4.2 million                │ 64          │ 32   │ 1536       │ OpenAI             │
└───────────┴────────────────────────────┴─────────────┴──────┴────────────┴────────────────────┘
```

- `snapshot` - This command creates a snapshot of an existing deployment (e.g. locally stored) and produces a file that can be used with `TBD` command to produce an estimate of the sizing required to host a Chroma server with extrapolated number of vectors. An example output is shown below:

- `extrapolate` - This command takes a snapshot files produced by `snapshot` command and produces an estimate of the sizing required to host a Chroma server with extrapolated number of vectors. An example output is shown below:

## **Proposed Changes**

### Concepts

**Documents** - while in Chroma we call a single collection entry a document for most users this is not the mental model. Therefore we suggest a way to establish through a series of questions what a document or what is the piece of information the user wants to store in Chroma (prior to chunking it) and what an average expected size (words or character length) of one such document is.

**Hardware Sizing Increments** - we suggest following standard cloud hardware sizing increments for two main reasons:

- These are the increments that most users will be familiar and will be presented with when considering Chroma deployments in Cloud setting
- Balanced CPU-to-Memory ratio

**Growth Factor** - this is compound metric that represents two things:

- Growth of documents over a period of time
- Growth of load (e.g. Load Factor + Concurrency Factor) over a period of time

**Load Factor** - this is a multiplier used to model variations in query complexity, data volume, temporary objects and partly concurrency. We suggest four simplified load factors:

**Concurrency Factor** - this is a multiplier used to model anticipated parallel queries in Chroma. The intuition is that as the higher the number of concurrent queries, the higher the number of temporary objects used by Chroma. This metric is mostly non-linear therefore we suggest four simplified concurrency factors:

This sip proposes an estimation formula that is based on the following factors:

- 💯 Number of documents - an estimated figure of the number of documents that the user wants to store in Chroma
- ⒩ Vector dimensionality - the size of the vector embeddings (in this iteration we assume uniform dimensionality across all vectors)
- 🧬 Embedding function - the embedding function used to generate the vectors (e.g. OpenAI, SentenceBERT, etc.)
- 📈 Growth factor - a multiplier used when the user wants an estimate over a period of time with given growth factor over an interval of the period (e.g. month over month growth of 10% for a period of 12 months)
- 🔥 Load Factor - a multiplier used to model the anticipated load on Chroma, this used to model variations in query complexity, data volume, temporary objects and partly concurrency. We suggest four simplified load factors:
  - Low - 1.0 (default value)
  - Medium - 1.2
  - High - 1.5
  - Burst - 2.0
- ⧥ Concurrency Factor - a multiplier used to model anticipated parallel queries in Chroma. The intuition is that as the higher the number of concurrent queries, the higher the number of temporary objects used by Chroma. This metric is mostly non-linear therefore we suggest four simplified concurrency factors:
  - Low - 1.0 (default value)
  - Medium - 1.2
  - High - 1.5
  - Burst - 2.0
- 🖥️ System Overhead - a multiplier used to model the overhead of the system (e.g. OS, Chroma server, etc.). We suggest a 30% overhead for the system.

Assumptions:

- We assume that the user will rely on default hnsw hyper parameter configuration (`hnsw:space=l2`, `hnsw:construction_ef=100`, `hnsw:search_ef=10`, `"hnsw:M=16`) (Note: in a future iteration we can add a way to specify custom hnsw hyper parameters to the estimate)
- We also assume default hnsw index batch parameters (`hnsw:batch_size=100`, `hnsw:sync_threshold=1000`)

The final formula is as follows:

`memory required = Vc * Vd * 4 bytes * Lf * Cf * Of`

Where:

- `Vc` - 💯 number of vectors
- `Vd` - ⒩ vector dimensionality
- `Lf` - 🔥 load factor
- `Cf` - ⧥ concurrency factor
- `Of` - 🖥️ system overhead factor

The required memory is then rounded up to the nearest hardware increment.

We suggest the following hardware increments:

- 1 vCPU / 0.5 GB RAM
- 1 vCPU / 1 GB RAM
- 1 vCPU / 2 GB RAM
- 2 vCPU / 4 GB RAM
- 4 vCPU / 8 GB RAM
- 8 vCPU / 16 GB RAM
- 16 vCPU / 32 GB RAM
- 32 vCPU / 64 GB RAM
- 64 vCPU / 128 GB RAM
- 128 vCPU / 256 GB RAM

> Note: The above list is not an exhaustive one but can serve as basis for calculation.

### Intuitions

Our base intuition for the estimation is that generally memory is more important than CPU for most Chroma Workloads, however as things with load and concurrency scale CPU becomes more important.
Another intuition we have is that disk IO may also be a bottleneck for some workloads, e.g. write heavy, but also some read heavy workloads such as multiple concurrent queries.
Intuition for the growth factor is that as most useful systems are rarely single-user or static and as such they grow in at least two dimensions - the data stored and the load on the system. Our core intuition about this is that data growth is a function of user growth which in turn models the load on the system.

### Future Work

The current proposal is for a single instance Chroma deployment. However in the future we'd want this to work for distributed Chroma where more factors are at play.

Benchmarking - we need to gather more data from real deployments to be able to provide more accurate sizing estimates.

HNSW parameters - in this CIP we assume defaults but in future we'd want to allow users to specify custom HNSW parameters.

## **Compatibility, Deprecation, and Migration Plan**

No compatibility issues.

## **Test Plan**

We will introduce a series of CLI tests to verify the correctness of the estimation tool.

## **Rejected Alternatives**

We have considered the following alternatives:

- 📝 Pen and Paper - 🤣
- 📊 Excel - While excel is a great tool for this kind of work we believe that a CLI is a more natural and developer friendly way.
