# Reader centroid precision comparison

360 measured rows. Index: 40M Wikipedia, 100 fixed exact L2 queries, 32 query workers.

The modes vary navigation precision only. All modes use the same saved tree, one-bit posting codes, and full-precision reranking. The root beam keeps all children. The next internal beam is 64 or 256; the final leaf beam is fixed independently.

## Lowest warm latency at each recall target

| Target recall | Mode | Internal beam | Leaf beam | Rerank factor | Actual recall | Leaf reachability | Codes scanned | Vectors reranked | Mean warm ms | Navigation ms | Cold filesystem MiB/query |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 80% | 1bit-4bit | 64 | 64 | 2 | 83.05% | 85.83% | 95439 | 200 | 7.50 | 1.41 | 254.22 |
| 80% | 4bit-4bit | 64 | 32 | 4 | 81.77% | 82.03% | 47784 | 400 | 18.42 | 13.43 | 415.63 |
| 80% | 4bit-fp | 64 | 32 | 4 | 81.79% | 82.05% | 47772 | 400 | 8.21 | 3.04 | 416.00 |
| 90% | 1bit-4bit | 64 | 128 | 8 | 90.05% | 90.18% | 190524 | 800 | 16.36 | 1.50 | 722.57 |
| 90% | 4bit-4bit | 64 | 128 | 4 | 90.82% | 91.43% | 190708 | 400 | 25.66 | 13.72 | 438.60 |
| 90% | 4bit-fp | 64 | 128 | 4 | 90.92% | 91.55% | 190894 | 400 | 15.23 | 3.14 | 438.61 |
| 94% | 1bit-4bit | 256 | 256 | 8 | 94.42% | 94.57% | 380460 | 800 | 28.43 | 4.34 | 724.88 |
| 94% | 4bit-4bit | 256 | 256 | 4 | 94.66% | 95.55% | 381003 | 400 | 69.45 | 48.55 | 441.49 |
| 94% | 4bit-fp | 256 | 256 | 4 | 94.67% | 95.55% | 381004 | 400 | 31.60 | 10.44 | 441.28 |

## Lowest scan count at each recall target

| Target recall | Mode | Internal beam | Leaf beam | Rerank factor | Recall | Codes scanned | Vectors reranked | Mean warm ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 80% | 1bit-4bit | 256 | 32 | 4 | 80.84% | 47734 | 400 | 9.30 |
| 80% | 4bit-4bit | 256 | 32 | 2 | 80.99% | 47778 | 200 | 51.63 |
| 80% | 4bit-fp | 256 | 32 | 2 | 80.81% | 47743 | 200 | 13.76 |
| 90% | 1bit-4bit | 256 | 128 | 4 | 91.05% | 190467 | 400 | 16.39 |
| 90% | 4bit-4bit | 256 | 128 | 4 | 92.23% | 190571 | 400 | 60.08 |
| 90% | 4bit-fp | 256 | 128 | 4 | 92.31% | 190713 | 400 | 22.37 |
| 94% | 1bit-4bit | 256 | 256 | 8 | 94.42% | 380460 | 800 | 28.43 |
| 94% | 4bit-4bit | 256 | 256 | 4 | 94.66% | 381003 | 400 | 69.45 |
| 94% | 4bit-fp | 256 | 256 | 4 | 94.67% | 381004 | 400 | 31.60 |

## Storage and interpretation

- 1bit-4bit: 3.85 MiB of logical centroid code payload.
- 4bit-4bit: 14.02 MiB of logical centroid code payload.
- 4bit-fp: 14.02 MiB of logical centroid code payload.

The experiment derives four-bit codes once from the saved original centroids and keeps them in reader memory. It retains the old codes too; reported logical centroid bytes describe replacing the one-bit storage payload, not the complete experimental reader RSS. Setup is excluded from query latency.

Cold clears reader postings and block pins; the 1 GiB provider cache and OS file cache can still contain data. Filesystem read traffic includes full storage blocks served through the OS cache, averaged across the concurrent 100-query batch. It is not production network traffic or isolated per-query cold bytes. Warm immediately repeats the same queries. The fixed query set and single exploratory samples do not establish general recall or latency variance.
