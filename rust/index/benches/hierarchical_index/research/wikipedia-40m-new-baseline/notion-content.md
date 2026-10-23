This run builds and persists an index for the first 40 million Wikipedia vectors on the current feature branch. It records every one-million-vector increment and becomes the reference for subsequent performance experiments. The four retained intermediate indexes contain 9M, 19M, 29M, and 39M vectors; the final index contains 40M.

## Results

The full process completes successfully in **43 minutes 56.57 seconds**. The CLI build summary is **42.5 minutes**, including final index diagnostics and harness overhead. The sum of the measured load, add, balance, commit, and reopen phases is **35.98 minutes**. The difference is not separately instrumented; it includes final diagnostics, garbage collection, snapshot handling, and measurement overhead. These timings should retain their distinct meanings in future comparisons.

<table header-row="true">
	<tr><td>Measured phase</td><td>Sum across 40 increments</td></tr>
	<tr><td>Load source vectors</td><td>3.72 min</td></tr>
	<tr><td>Add vectors</td><td>6.47 min</td></tr>
	<tr><td>Balance the tree</td><td>16.21 min</td></tr>
	<tr><td>Persist changed blocks</td><td>9.48 min</td></tr>
	<tr><td>Reopen the saved writer</td><td>5.97 s</td></tr>
</table>

The final increment adds its million vectors in 13.45 seconds, balances in 43.03 seconds across eight rounds, and commits in 20.77 seconds. It loads source vectors in 5.65 seconds and reopens in 461 milliseconds. The process peaks at **209.89 GiB RSS** and averages **12.13 CPU cores**. The final tree reports 40M valid unique vectors and zero orphans.

The saved same-machine reference run takes 9 hours 13 minutes 40 seconds including recall, versus 43 minutes 56.57 seconds here: an observed **12.6× reduction in elapsed time**. Its source is `22eb704`, while this run includes both metadata-block version caching and parallel source-shard decoding. Both runs use the same approved writer settings and read sweep. This comparison measures their aggregate effect; it does not isolate an individual change or establish repeat-run variance.

## Comparison with historical runs

Index computation means placing new vectors and restoring the tree after insertion. The graph compares add plus balancing for the two hierarchical runs with integrated index time for the quantized implementation. It excludes input decoding and commits from that common measure. Missing historical phases remain absent rather than becoming zeros.

CHART_ATTACHMENT

<table header-row="true">
	<tr><td>Through 40M vectors</td><td>New baseline</td><td>Historical hierarchical</td><td>Historical quantized</td></tr>
	<tr><td>Index computation</td><td>22.68 min</td><td>approximately 337.63 min</td><td>approximately 86.24 min</td></tr>
	<tr><td>Commit</td><td>9.48 min</td><td>Not reported</td><td>approximately 51.87 min</td></tr>
	<tr><td>Final increment computation</td><td>56.48 s</td><td>approximately 1110 s</td><td>approximately 162 s</td></tr>
	<tr><td>Final increment balancing</td><td>43.03 s</td><td>approximately 570 s</td><td>Included in index time</td></tr>
</table>

The historical hierarchical page is <mention-page url="https://app.notion.com/p/34458a6d8191808b8f9ed1330132c1b0"/>. It uses 32 threads and write minimum percentages of 50, 5, and 0, which the new run omits. It does not document hardware or equivalent commit/reopen behavior and reports 579 orphaned vectors. Its minute-valued timings are rounded.

The quantized reference is the “42M Wikipedia en split full” section at <mention-page url="https://app.notion.com/p/34558a6d81918068b089d1e0e2f51025"/>. The graph uses its actual 1M through 40M rows from the 41.49M run. Its split/merge thresholds are 512/128, it requests two replicas and RNG factor 4, and its hardware/thread count is unstated. The new run uses thresholds 2048/512, one replica, and RNG factor 0. These are historical observations with different configurations, not a matched hardware or equal-recall experiment.

## Reads

Recall measures the fraction of the exact nearest 100 vectors returned across 100 fixed queries. The 1× setting performs no full-precision reranking; 8× checks additional candidates with original vectors before returning the nearest 100. Warm latency is the immediate second query pass. “Cold” clears application buffers between rows but retains the OS file cache, so cold results depend on preceding rows and do not represent a production network cache miss.

<table header-row="true">
	<tr><td>Search tau</td><td>Rerank</td><td>Recall at 100</td><td>Cold mean latency</td><td>Warm mean latency</td></tr>
	<tr><td>0.70</td><td>1x</td><td>59.44%</td><td>215.3ms</td><td>1.6ms</td></tr>
	<tr><td>0.70</td><td>8x</td><td>66.33%</td><td>772.5ms</td><td>9.6ms</td></tr>
	<tr><td>1.00</td><td>1x</td><td>59.44%</td><td>11.3ms</td><td>1.9ms</td></tr>
	<tr><td>1.00</td><td>8x</td><td>66.33%</td><td>731.5ms</td><td>8.4ms</td></tr>
	<tr><td>2.00</td><td>1x</td><td>75.45%</td><td>78.9ms</td><td>21.5ms</td></tr>
	<tr><td>2.00</td><td>8x</td><td>94.38%</td><td>810.9ms</td><td>29.7ms</td></tr>
</table>

## Checkpoints and validation

The selected snapshots preserve unchanged file manifests after later commits and garbage collection. Independent metadata files refer to immutable blocks that share disk space. Changed blocks and roots receive new UUIDs, so later writes create new files; deleting an unselected snapshot only removes its directory entries. The early retention test verifies identical content hashes for 1M and 2M after a later 3M commit and successfully reopens both.

The retained paths are `/mnt/data/wikipedia-40m-baseline-20261009-snapshots/9000000`, `/19000000`, `/29000000`, and `/39000000`. The final output is `/mnt/data/wikipedia-40m-baseline-20261009`. The four snapshots share 171.32 GiB of unique data; final 40M adds its new blocks. Snapshot creation is outside printed checkpoint phase timings, but the full process includes it. Unselected snapshots are pruned after their completion marker; their manifests remain in the results directory. Source vectors, exact ground truth, and all benchmark evidence remain preserved.

## Reproduction and complete output

CLI_ATTACHMENT

PATCH_ATTACHMENT

CSV_ATTACHMENT

The machine is an AWS r6id.16xlarge with 64 vCPUs, 512 GiB nominal memory, and Intel Xeon Platinum 8375C processors. The run uses 32 add workers and 32 balancing workers. Source shards and block files are cached on local NVMe; the operating system file cache is not dropped. These results measure local filesystem behavior rather than production network latency.

The production source is `f3ac04b1ae9df1025cf0a960d5a91d9b451f6d18`, which includes merged PR #7874 and the parallel loading change #7872. Harness-only snapshot retention is commit `bec6e7213b37ca97066545535bc82d4252f9c0dc`; it changes no index algorithm. The executable SHA256 is `a3bd63919af743118373aa6b17c7f80d27d522dcff0a4f9ad56a12b1cc35af8c`. The complete CLI output SHA256 is `0ecd0621e20553f0cccb8dda11d743502592044dee20a583fd80fd8635af7a5d`.

The dataset is the ordered English prefix of `CohereLabs/wikipedia-2023-11-embed-multilingual-v3`, with 1024 dimensions. Exact answers are precomputed for the same 100 queries at each million-vector boundary using float64 distance accumulation. Their saved ground-truth SHA256 is `d4863b7df4213fb44f007180c99f5f65c14d19573d0af6473e2028602dcbdfdf`. Data download, ground-truth generation, and compilation are excluded from the timed process.

```sh
/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline/binary --dataset wikipedia-en --metric l2 --checkpoint 40 --checkpoint-size 1000000 --branching-factor 100 --split-threshold 2048 --merge-threshold 512 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _\,_\,0.30 --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0 --read-beam-min 10 --read-beam-max 256 --read-level-taus _\,_\,_ --read-level-min-pcts 50\,5\,0 --recall-rerank-vectors 1\,8 --recall-tau-values 0.7\,1\,2 --num-queries 100 --max-cache-bytes 1073741824 --compute-gt-clusters false --save --save-dir /mnt/data/wikipedia-40m-baseline-20261009 --checkpoint-snapshots-dir /mnt/data/wikipedia-40m-baseline-20261009-snapshots
```

The complete output starts with `Loading Wikipedia EN from HuggingFace Hub...` and is saved as `full-cli-output.txt`. It contains every checkpoint and the full read sweep. Local evidence resides beside this report; the remote directory is `/home/ubuntu/benchmark-data/wikipedia-40m-new-baseline`. It includes command/source/binary hashes, resource measurements, checkpoint manifests, audits, pruning records, and the snapshot harness patch.

## Every checkpoint

Times below are seconds; reopen is expressed in seconds even when the CLI prints milliseconds.

<table header-row="true">
	<tr><td>Index size</td><td>Load</td><td>Add</td><td>Balance</td><td>Rounds</td><td>Commit</td><td>Reopen</td></tr>
	<tr><td>1M</td><td>4.95</td><td>3.26</td><td>27.11</td><td>20</td><td>5.48</td><td>0.006</td></tr>
	<tr><td>2M</td><td>5.10</td><td>6.47</td><td>10.20</td><td>3</td><td>6.36</td><td>0.010</td></tr>
	<tr><td>3M</td><td>5.08</td><td>7.21</td><td>10.28</td><td>2</td><td>6.93</td><td>0.016</td></tr>
	<tr><td>4M</td><td>5.51</td><td>6.94</td><td>9.12</td><td>1</td><td>7.32</td><td>0.022</td></tr>
	<tr><td>5M</td><td>5.52</td><td>7.32</td><td>10.84</td><td>1</td><td>7.63</td><td>0.024</td></tr>
	<tr><td>6M</td><td>5.46</td><td>7.26</td><td>12.99</td><td>2</td><td>8.17</td><td>0.037</td></tr>
	<tr><td>7M</td><td>5.58</td><td>7.45</td><td>12.59</td><td>1</td><td>8.64</td><td>0.044</td></tr>
	<tr><td>8M</td><td>5.46</td><td>7.46</td><td>12.88</td><td>2</td><td>8.98</td><td>0.039</td></tr>
	<tr><td>9M</td><td>5.48</td><td>7.51</td><td>18.30</td><td>1</td><td>8.97</td><td>0.055</td></tr>
	<tr><td>10M</td><td>5.49</td><td>7.68</td><td>14.60</td><td>1</td><td>9.26</td><td>0.070</td></tr>
	<tr><td>11M</td><td>5.43</td><td>7.87</td><td>15.45</td><td>2</td><td>9.96</td><td>0.075</td></tr>
	<tr><td>12M</td><td>5.48</td><td>7.91</td><td>16.62</td><td>1</td><td>10.64</td><td>0.060</td></tr>
	<tr><td>13M</td><td>5.41</td><td>8.23</td><td>19.36</td><td>2</td><td>11.44</td><td>0.099</td></tr>
	<tr><td>14M</td><td>5.51</td><td>8.25</td><td>24.52</td><td>8</td><td>11.43</td><td>0.073</td></tr>
	<tr><td>15M</td><td>5.54</td><td>8.54</td><td>26.30</td><td>8</td><td>11.93</td><td>0.115</td></tr>
	<tr><td>16M</td><td>5.51</td><td>8.74</td><td>20.76</td><td>3</td><td>11.99</td><td>0.126</td></tr>
	<tr><td>17M</td><td>5.46</td><td>8.65</td><td>19.93</td><td>2</td><td>12.73</td><td>0.087</td></tr>
	<tr><td>18M</td><td>5.49</td><td>9.04</td><td>19.85</td><td>2</td><td>12.91</td><td>0.088</td></tr>
	<tr><td>19M</td><td>5.58</td><td>9.33</td><td>20.89</td><td>2</td><td>13.43</td><td>0.095</td></tr>
	<tr><td>20M</td><td>5.53</td><td>9.26</td><td>22.09</td><td>3</td><td>13.93</td><td>0.105</td></tr>
	<tr><td>21M</td><td>5.51</td><td>9.54</td><td>23.99</td><td>3</td><td>14.19</td><td>0.177</td></tr>
	<tr><td>22M</td><td>5.53</td><td>10.07</td><td>22.37</td><td>2</td><td>14.84</td><td>0.198</td></tr>
	<tr><td>23M</td><td>5.71</td><td>10.02</td><td>24.15</td><td>4</td><td>14.76</td><td>0.233</td></tr>
	<tr><td>24M</td><td>5.51</td><td>10.15</td><td>25.68</td><td>1</td><td>15.21</td><td>0.144</td></tr>
	<tr><td>25M</td><td>5.44</td><td>10.73</td><td>25.82</td><td>2</td><td>16.32</td><td>0.154</td></tr>
	<tr><td>26M</td><td>4.99</td><td>10.79</td><td>27.26</td><td>3</td><td>16.91</td><td>0.156</td></tr>
	<tr><td>27M</td><td>6.06</td><td>11.20</td><td>28.13</td><td>2</td><td>16.59</td><td>0.175</td></tr>
	<tr><td>28M</td><td>5.52</td><td>11.19</td><td>26.94</td><td>2</td><td>17.13</td><td>0.163</td></tr>
	<tr><td>29M</td><td>6.27</td><td>11.52</td><td>35.95</td><td>8</td><td>17.38</td><td>0.333</td></tr>
	<tr><td>30M</td><td>5.45</td><td>11.78</td><td>37.01</td><td>8</td><td>18.15</td><td>0.356</td></tr>
	<tr><td>31M</td><td>6.18</td><td>11.79</td><td>37.96</td><td>8</td><td>18.33</td><td>0.200</td></tr>
	<tr><td>32M</td><td>5.52</td><td>11.83</td><td>34.70</td><td>8</td><td>19.14</td><td>0.339</td></tr>
	<tr><td>33M</td><td>5.54</td><td>12.15</td><td>37.45</td><td>8</td><td>18.95</td><td>0.210</td></tr>
	<tr><td>34M</td><td>6.03</td><td>12.26</td><td>29.47</td><td>2</td><td>19.33</td><td>0.220</td></tr>
	<tr><td>35M</td><td>6.22</td><td>12.47</td><td>30.52</td><td>2</td><td>20.03</td><td>0.225</td></tr>
	<tr><td>36M</td><td>6.15</td><td>12.88</td><td>32.89</td><td>2</td><td>20.12</td><td>0.234</td></tr>
	<tr><td>37M</td><td>5.76</td><td>13.10</td><td>33.85</td><td>3</td><td>22.56</td><td>0.243</td></tr>
	<tr><td>38M</td><td>5.59</td><td>13.46</td><td>32.32</td><td>2</td><td>24.63</td><td>0.246</td></tr>
	<tr><td>39M</td><td>5.80</td><td>13.47</td><td>38.58</td><td>8</td><td>25.38</td><td>0.257</td></tr>
	<tr><td>40M</td><td>5.65</td><td>13.45</td><td>43.03</td><td>8</td><td>20.77</td><td>0.461</td></tr>
</table>
