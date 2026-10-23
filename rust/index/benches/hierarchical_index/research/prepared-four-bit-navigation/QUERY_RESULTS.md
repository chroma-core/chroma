# Prepared four-bit query comparison

Each row averages three runs on the saved 40M Wikipedia index with 100 exact L2 queries and 32 workers. Cold clears reader postings and block pins; provider and OS caches remain. Filesystem bytes are averaged over the concurrent query batch and do not measure production network traffic.

| Centroid / query implementation | Internal beam | Leaves | Reranked | Reachable recall | Final recall | Codes scanned | Warm mean ms | Navigation ms | Cold mean ms | Cold filesystem MiB/query |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1bit-4bit | 64 | 112 | 400 | 89.39% | 88.88% | 166,841 | 12.50 | 1.48 | 629.13 | 437.09 |
| 1bit-4bit | 64 | 128 | 800 | 90.18% | 90.05% | 190,524 | 16.28 | 1.48 | 977.81 | 722.47 |
| 1bit-4bit | 256 | 192 | 800 | 93.40% | 93.26% | 285,347 | 23.73 | 4.34 | 946.32 | 724.00 |
| 1bit-4bit | 256 | 240 | 400 | 94.27% | 93.47% | 356,572 | 24.45 | 4.38 | 524.19 | 440.94 |
| 1bit-4bit | 256 | 256 | 800 | 94.57% | 94.42% | 380,460 | 28.54 | 4.42 | 950.69 | 724.70 |
| 4bit-packed-4bit | 64 | 112 | 400 | 90.89% | 90.33% | 166,885 | 12.67 | 1.62 | 508.90 | 437.06 |
| 4bit-packed-4bit | 64 | 128 | 800 | 91.43% | 91.31% | 190,708 | 16.74 | 1.66 | 943.67 | 722.71 |
| 4bit-packed-4bit | 256 | 192 | 800 | 94.61% | 94.45% | 285,934 | 24.32 | 4.90 | 935.95 | 723.37 |
| 4bit-packed-4bit | 256 | 240 | 400 | 95.39% | 94.51% | 357,253 | 25.30 | 5.02 | 526.66 | 441.42 |
| 4bit-packed-4bit | 256 | 256 | 800 | 95.55% | 95.39% | 381,003 | 29.09 | 4.99 | 953.09 | 724.50 |
| 4bit-packed-fp | 64 | 112 | 400 | 90.85% | 90.28% | 167,056 | 12.67 | 1.60 | 571.27 | 436.58 |
| 4bit-packed-fp | 64 | 128 | 800 | 91.55% | 91.43% | 190,894 | 16.60 | 1.64 | 965.80 | 723.06 |
| 4bit-packed-fp | 256 | 192 | 800 | 94.59% | 94.44% | 285,831 | 24.21 | 4.88 | 941.23 | 723.55 |
| 4bit-packed-fp | 256 | 240 | 400 | 95.40% | 94.51% | 357,395 | 25.28 | 4.85 | 528.38 | 441.24 |
| 4bit-packed-fp | 256 | 256 | 800 | 95.55% | 95.38% | 381,004 | 28.93 | 4.79 | 953.99 | 724.41 |
| 4bit-planes-4bit | 64 | 112 | 400 | 90.89% | 90.33% | 166,885 | 12.88 | 1.82 | 516.91 | 437.13 |
| 4bit-planes-4bit | 64 | 128 | 800 | 91.43% | 91.31% | 190,708 | 16.90 | 1.88 | 946.08 | 722.66 |
| 4bit-planes-4bit | 256 | 192 | 800 | 94.61% | 94.45% | 285,934 | 25.42 | 6.01 | 932.01 | 723.39 |
| 4bit-planes-4bit | 256 | 240 | 400 | 95.39% | 94.51% | 357,253 | 26.13 | 5.96 | 518.55 | 441.43 |
| 4bit-planes-4bit | 256 | 256 | 800 | 95.55% | 95.39% | 381,003 | 30.04 | 5.93 | 949.70 | 724.45 |
