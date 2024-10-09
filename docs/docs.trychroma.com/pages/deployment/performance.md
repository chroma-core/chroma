---
title: "Single-Node Chroma: Performance and Limitations"
---

The single-node version of Chroma is designed to be easy to deploy and maintain, while still providing robust performance that satisfies a broad range of production applications.

To help you understand when single-node Chroma is a good fit for your use case, we have performed a series of stress tests and performance experiments to probe the system’s capabilities and discover its limitations and edge cases. We analyzed these boundaries across a range of hardware configurations, to determine what sort of deployment is appropriate for different workloads.

This document describes these findings, as well as some general principles for getting the most  out of your Chroma deployment.

## Results Summary

Roughly speaking, here is the sort of performance you can expect from Chroma on different EC2 instance types with a very typical workload:

- 1024 dimensional embeddings
- Small documents (100-200 words)
- Three metadata fields per record.

{% special_table %}
{% /special_table %}

| Instance Type   | System RAM | Approx. Max Collection Size | Mean Latency (insert) | 99.9% Latency (insert) | Mean Latency (query) | 99.9% Latency (query) | Monthly Cost |
|-----------------|------------|-----------------------------|-----------------------|------------------------|----------------------|-----------------------|--------------|
| **t3.small**    | 2          | 250,000                     | 55ms                  | 250ms                  | 22ms                 | 72ms                  | $15.936      |
| **t3.medium**   | 4          | 700,000                     | 37ms                  | 120ms                  | 14ms                 | 41ms                  | $31.072      |
| **t3.large**    | 8          | 1,700,000                   | 30ms                  | 100ms                  | 13ms                 | 35ms                  | $61.344      |
| **t3.xlarge**   | 16         | 3,600,000                   | 30ms                  | 100ms                  | 13ms                 | 30ms                  | $121.888     |
| **t3.2xlarge**  | 32         | 7,500,000                   | 30ms                  | 100ms                  | 13ms                 | 30ms                  | $242.976     |
| **r7i.2xlarge** | 64         | 15,000,000                  | 13ms                  | 50ms                   | 7ms                  | 13ms                  | $386.944     |

{% br %}{% /br %}

Deploying Chroma on a system with less than 2GB of RAM is **not** recommended.

Note that the latency figures in this table are for small collections. Latency increases as collections grow: see [Latency and collection size](./performance#latency-and-collection-size) below for a full analysis.

## Memory and collection size

Chroma uses a fork of [`hnswlib`](https://github.com/nmslib/hnswlib) to efficiently index and search over embedding vectors. The HNSW algorithm requires that the embedding index reside in system RAM to query or update.

As such, the amount of available system memory defines an upper bound on the size of a Chroma collection (or multiple collections, if they are being used concurrently.) If a collection grows larger than available memory, insert and query latency spike rapidly as the operating system begins swapping memory to disk. The memory layout of the index is not amenable to swapping, and the system quickly becomes unusable.

Therefore, users should always plan on having enough RAM provisioned to accommodate the anticipated total number of embeddings.

To analyze how much RAM is required, we launched an an instance of Chroma on variously sized EC2 instances, then inserted embeddings until each system became non-responsive. As expected, this failure point corresponded linearly to RAM and embedding count.

For 1024 dimensional embeddings, with three metadata records and a small document per embedding, this works out to `N = R * 0.245` where `N` is the max collection size in millions, and `R` is the amount of system RAM required in gigabytes. Remember, you wil also need reserve at least a gigabyte for the system’s other needs, in addition to the memory required by Chroma.

This pattern holds true up through about 7 million embeddings, which is as far as we tested. At this point Chroma is still fast and stable, and we did not find a strict upper bound on the size of a Chroma database.

## Disk space and collection size

Chroma durably persists each collection to disk. The amount of space required is a combination of the space required to save the HNSW embedding index, and the space required by the sqlite database used to store documents and embedding metadata.

The calculations for persisting the HNSW index are similar to that for calculating RAM size. As a rule of thumb, just make sure a system’s storage is at least as big as its RAM, plus several gigabytes to account for the overhead of the operating system and other applications.

The amount of space required by the sqlite database is highly variable, and depends entirely on whether documents and metadata are being saved in Chroma, and if so, how large they are. Fully exploring all permutations of this are beyond the scope of the experiments we were able to run.

However, as a single data point, the sqlite database for a collection with ~40k documents of 1000 words each, and ~600k metadata entries was about 1.7gb.

There is no strict upper bound on the size of the metadata database: sqlite itself supports databases into the terabyte range, and can page to disk effectively.

In most realistic use cases, it’s likely that the size and performance of the HNSW index in RAM becomes the limiting factor on a Chroma collection’s size long before the metadata database does.

## Latency and collection size

As collections get larger and the size of the index grows, inserts and queries both take longer to complete. The rate of increase starts out fairly flat then grow roughly linearly, with the inflection point and slope depending on the quantity and speed of CPUs available.

### Query Latency

![query-latency](/img/query-latency.png)

### Insert Latency

![insert-latency](/img/insert-latency.png)

{% note type="tip" title="" %}
If you’re using multiple collections, performance looks quite similar, based on the total number of embeddings across collections. Splitting collections into multiple smaller collections doesn’t help, but it doesn’t hurt, either, as long as they all fit in memory at once.
{% /note %}

## Concurrency

Although aspects of HNSW’s algorithm are multithreaded internally, only one thread can read or write to a given index at a time. For the most part, single-node Chroma is fundamentally single threaded. If a  operation is executed while another is still in progress, it blocks until the first one is complete.

This means that under concurrent  load, the average latency of each request will increase.

When writing, the increased latency is more pronounced with larger batch sizes, as the system is more completely saturated. We have experimentally verified this: as the number of concurrent writers is increased, average latency increases linearly.

![concurrent-writes](/img/concurrent-writes.png)

![concurrent-queries](/img/concurrent-queries.png)

Despite the effect on latency, Chroma does remain stable with high concurrent load. Too many concurrent users can eventually increase latency to the point where the system does not perform acceptably, but this typically only happens with larger batch sizes. As the above graphs shows, the system remains usable with dozens to hundreds of operations per second.

See the [Insert Throughput](./performance#insert-throughput) section below for a discussion of optimizing user count for maximum throughput when the concurrency is under your control, such as when inserting bulk data.

# CPU speed, core count & type

As a CPU bound application, it’s not surprising that CPU speed and type makes a difference for average latency.

As the data demonstrates, although it is not fully parallelized, Chroma can still take some advantage of multiple CPU cores for better throughput.

![cpu-mean-query-latency](/img/cpu-mean-query-latency.png)

{% note type="tip" title="" %}
Note the slightly increased latency for the t3.2xlarge instance. Logically, it should be faster than the other t3 series instances, since it has the same class of CPU, and more of them.

This data point is left in as an important reminder that the performance of EC2 instances is slightly variable, and it’s entirely possible to end up with an instance that has performance differences for no discernible reason.
{% /note %}

# Insert Throughput

A question that is often relevant is: given bulk data to insert, how fast is it possible to do so, and what’s the best way to insert a lot of data quickly?

The first important factor to consider is the number of concurrent insert requests.

As mentioned in the [Concurrency](./performance#concurrency) section above, actual insertion throughput does not benefit from concurrency. However, there is some amount of network and HTTP overhead which can be parallelized. Therefore, to saturate Chroma while keeping latencies as low as possible, we recommend 2 concurrent client processes or threads inserting as fast as possible.

The second factor to consider is the batch size of each request. Performance is mostly linear with respect to batch size, with a constant overhead to process the HTTP request itself.

Experimentation confirms this: overall throughput (total number of embeddings inserted, across batch size and request count) remains fairly flat between batch sizes of 100-500:

![concurrent-inserts](/img/concurrent-inserts.png)

Given that smaller batches have lower, more consistent latency and are less likely to lead to timeout errors, we recommend batches on the smaller side of this curve: anything between 50 and 250 is a reasonable choice.

## Conclusion

Users should feel comfortable relying on Chroma for use cases approaching tens of millions of embeddings, when deployed on the right hardware. It’s average and upper-bound latency for both reads and writes make it a good platform for all but the largest AI-based applications, supporting potentially thousands of simultaneous human users (depending on your application’s backend access patterns.)

As a single-node solution, though, it won’t scale forever. If you find your needs exceeding the parameters laid out in this analysis, we are extremely interested in hearing from you. Please fill out [this form](https://airtable.com/appqd02UuQXCK5AuY/pagr1D0NFQoNpUpNZ/form), and we will add you to a dedicated Slack workspace for supporting production users. We would love to help you think through the design of your system, whether Chroma has a place in it, or if you would be a good fit for our upcoming distributed cloud service. You can also join the [#production-chroma](https://discord.com/channels/1073293645303795742/1292554909694300211) channel on Discord to join our community!
