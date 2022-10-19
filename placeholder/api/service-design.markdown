# Chroma Service API Design

## Overview

Proposing an architecture for the internal Chroma service operation.

These functions will let us control and inspect a running Chroma service, which may be spread across different machines and processes.  It aims to help us understand, diagnose, and debug the system during early development, as well as lead us to future service development.

## Components

All of these components might find a place.  Many of the python components can live together in the same process in the beginning, and only separate as necessary.

| Component | Type | Description |
| ----------- | ----------- | ----------- |
| Training Datasets | Maybe parquet | Write once |
| Prod Datasets |  | Might write in a stream |
| Reccomendation Engine | Python | Processes data from other components |
| Query Engine | Combined with db in Milvus, or custom | |
| ANN Indexer | Combined with db in Milvus, or more like HNSW | |
| Custom Indexer | E.g. MHB, other future algorithms | |
| Generic Database | Something simple, could be SQLite | User data, notification statuses, statistics |
| API Server | FastAPI | Handle logging and queries |
| Service Control | FastAPI | Interface to manage other components |
| Web server | FastAPI | Back end for React in the future |

# Component descriptions

## Training Datasets

We write these once, then index them.  It will be rare to re-index, so it can be expensive.  Old data will become much less interesting as new data comes in, suggesting either expiration, or the implicit cheap storage of parquet.

## Prod Datasets

These can be written to for a long time, with a lot of records.  

Indexes must be updated (how frequently?) based on new data.

User may want to tag or delete records.

## Reccomendation Engine

Chroma special sauce.  Process data from the other components, especially `Query Engine` and `both Datasets`.

Maintain (probably in `Generic Database component`) a sorted list of recommendations in descending quality order.

Runs as a python service.  

## Query Engine

Previously-indexed vector queries across all data for a set, both training and prod.  ANN, etc.

Possibly running in-memory with a library like HNSW.

Possibly combined with the database in Milvus.

## ANN Indexer

In file mode, re-index all of a dataset (train + prod) periodically, write out index for `Query Engine`

Possibly simplified by a db combo like Milvus.

## Custom Indexer

E.g. MHB that indexes training data only.

Can include future indexes that cover training + prod.

Notably, these can run in separate processes via python `multiprocessing`.  That can simplify their interfaces, and make them easy to test and operate separately.

## Generic Database

Handles all the routine data tasks, including, but not limited to:

- User data, preferences
- Metadata about datasets
- Recommendations
- Notifications and statusees
- Reports

## API Server

Handle logging and queries.

Must be fast and reliable.

Minimize dependencies.

Probably FastAPI.

## Service Control

As an open source service with potentially a lot of separate components, a single control system can make a big difference in ease of adoption and operation.

Manage all of the other services, probably over HTTP.  

Start, stop, and run operations.

## Web server

Deferred for now, but when we have a user web interface, this service will support its back end features, collecting data from the other components in one place.

Probably FastAPI

# Internal objects

Especially in our early versions, some of these functions can be thought of as libraries running in a main process.

We'll try to maintain the right amount of separation between areas, to make it easier to swap out different implementations, and move to new processes.

## API Server

Serve endpoints in `chroma-api.yaml`.

```
/log
    - validate entry
    - compute embedding_set_key
    - commit entry to disk (method TBD)
    - respond 201 to client
```

We can possibly trigger new work on successful /log calls:

###  Rewrite committed entries to secondary storage

For example, we could write the first records out in JSONL, buffer a large chunk, then roll up that chunk into arrow/parquet.

### Trigger indexing based on prod logs

We expect the user to:

1. log training
2. log prod
3. ask for answers

Once we see a prod message logged, we can check for the ANN index.  If it's not there, or out of date, we can queue a new ANN index.

Keeping this implicit, instead of requiring the user to make an indexing call, lets Chroma experiment with new algorithms and triggers.
