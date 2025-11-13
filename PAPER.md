# SPANN: Highly-efficient Billion-scale Approximate Nearest Neighbor Search

**Qi Chen¹,*** **Bing Zhao¹,²,†** **Haidong Wang¹** **Mingqin Li¹** **Chuanjie Liu¹,³,†**
**Zengzhong Li¹** **Mao Yang¹** **Jingdong Wang¹,⁴,*,†**

¹Microsoft ²Peking University ³Tencent ⁴Baidu

¹{cheqi, haidwa, mingqli, jasol, maoyang}@microsoft.com
²its.bingzhao@pku.edu.cn ³liu.chuanjie@outlook.com
⁴wangjingdong@outlook.com

*Corresponding author.
†Work done while at Microsoft.

35th Conference on Neural Information Processing Systems (NeurIPS 2021).

## Abstract

The in-memory algorithms for approximate nearest neighbor search (ANNS) have achieved great success for fast high-recall search, but are extremely expensive when handling very large scale database. Thus, there is an increasing request for the hybrid ANNS solutions with small memory and inexpensive solid-state drive (SSD). In this paper, we present a simple but efficient memory-disk hybrid indexing and search system, named SPANN, that follows the inverted index methodology. It stores the centroid points of the posting lists in the memory and the large posting lists in the disk. We guarantee both disk-access efficiency (low latency) and high recall by effectively reducing the disk-access number and retrieving high-quality posting lists. In the index-building stage, we adopt a hierarchical balanced clustering algorithm to balance the length of posting lists and augment the posting list by adding the points in the closure of the corresponding clusters. In the search stage, we use a query-aware scheme to dynamically prune the access of unnecessary posting lists. Experiment results demonstrate that SPANN is 2× faster than the state-of-the-art ANNS solution DiskANN to reach the same recall quality 90% with same memory cost in three billion-scale datasets. It can reach 90% recall@1 and recall@10 in just around one millisecond with only 32GB memory cost. Code is available at: https://github.com/microsoft/SPTAG.

## 1 Introduction

Vector nearest neighbor search has played an important role in information retrieval area, such as multimedia search and web search, which provides relevant results by searching vectors with minimum distance to the query vector. Exact solutions for K-nearest neighbor search [49, 40] are not applicable in big data scenario due to substantial computation cost and high query latency. Therefore, researchers have proposed many kinds of approximate nearest neighbor search (ANNS) algorithms in the literature [11, 18, 38, 10, 14, 31, 34, 13, 29, 21, 16, 26, 42, 43, 33, 44, 37, 32, 19, 27, 9, 12, 39, 50, 20, 36]. However, most of the algorithms mainly focus on how to do low latency and high recall search all in memory with offline pre-built indexes. When targeting to the super large scale vector search scenarios, such as web search, the memory cost will become extremely expensive. There is an increasing request for the hybrid ANNS solutions that use small memory and inexpensive disk to serve the large scale datasets.

There are only a few approaches working on the hybrid ANNS solutions, including DiskANN [39] and HM-ANN [36]. Both of them are graph based solutions. DiskANN uses Product Quantization (PQ) [25] to compress the vectors stored in the memory while putting the navigating spread-out graph along with the full-precision vectors on the disk. When a query comes, it traverses the graph according to the distance of quantized vectors and then reranks the candidates according to distance of the full-precision vectors. HM-ANN leverages the heterogeneous memory by placing pivot points in the fast memory and navigable small world graph in the slow memory without data compression. However, it consumes more than 1.5 times larger fast memory than DiskANN. Moreover, the slow memory is still much expensive than disk. Therefore, due to the cheap serving cost, high recall and low latency advantages of DiskANN, it has become the start-of-the-art for indexing billion-scale datasets.

In this paper, we argue that the simple inverted index approach can also achieve state-of-the-art performance for large scale datasets in terms of recall, latency and memory cost. We propose SPANN, a simple but surprising efficient memory-disk hybrid vector indexing and search system, that follows the inverted index methodology. SPANN only stores the centroid points of the posting lists in the memory while putting the large posting lists in the disk. We guarantee both low latency and high recall by greatly reducing the number of disk accesses and improving the quality of posting lists. In the index-building stage, we use a hierarchical balanced clustering method to balance the length of posting lists and expand the posting list by adding the points in the closure of the corresponding clusters. In the search stage, we use a query-aware scheme to dynamically prune the access of unnecessary posting lists. Experiment results demonstrate that SPANN is more than two times faster than the state-of-the-art disk-based ANNS algorithm DiskANN to reach the same recall quality 90% with same memory cost in three billion-scale datasets. It can reach 90% recall@1 and recall@10 in just around one millisecond with only 10% of original memory cost. SPANN has already been deployed into Microsoft Bing to support hundreds of billions scale vector search.

## 2 Background and Related Work

Given a set of data vectors X ∈ R^(n×m) (the data set contains n vectors with m-dimensional features) and a query vector q ∈ R^m, the goal of vector search is to find a vector p* from X, called nearest neighbor, such that p* = arg min_(p∈X) Dist(p, q). Similarly, we can define K-nearest neighbors. Due to the substantial computation cost and high query latency of the exhaustive search, ANNS algorithms are designed to speedup the search for the approximiate K-nearest neighbors in a large dataset in an acceptable amount of time. Most of the ANNS algorithms in the literature mainly focus on the fast high-recall search in the memory, including hash based methods [14, 24, 47, 48, 45, 46, 51], tree based methods [11, 31, 44, 33], graph based methods [21, 16, 42, 32], and hybrid methods [43, 12, 23, 22]. However, with the explosive growth of the vector scale, the memory has become the bottleneck to support large scale vector search. There are only a few approaches working on the ANNS solutions for billon-scale datasets to minimize the memory cost. They can be divided into two categories: inverted index based and graph based methods.

The inverted index based methods, such as IVFADC [26], FAISS [27] and IVFOADC+G+P [9], split the vector space into K Voronoi regions by KMeans clustering and only do search in a few regions that are closed to the query. To reduce the memory cost, they use vector quantization, e.g. Product Quantization (PQ) [25], to compress the vectors and store them in the memory. The inverted multi-index (IMI) [7] also uses PQ to compress vectors. It splits the feature space into multiple orthogonal subspaces and constructs a separate codebook for each subspace. The full feature space is produced as a Cartesian product of the corresponding subspaces. Multi-LOPQ[28] uses locally optimized PQ codebook to encode the displacements in the IMI structure. GNO-IMI [8] optimizes the IMI by using non-orthogonal codebooks to produce the centroids. Although they can cut down the memory usage to less than 64GB for one billion 128 dimensional vectors, the recall@1 is very low (only around 60%) due to lossy data compression. Although they can achieve better recall by returning 10 to 100 times more candidates for further reranking, it is often not acceptable in many scenarios.

The graph based methods include DiskANN [39] and HM-ANN [36]. Both of them adopt the hybrid solution. DiskANN also stores the PQ compressed vectors in the memory while storing the navigating spread-out graph along with the full-precision vectors on the disk. When a query comes, it traverses the graph using best-first manner according to the distance of quantized vectors and then reranks the candidates according to distance of the full-precision vectors. Similarly, it uses the lossy data compression which will influence the recall quality even though full-precision vector reranking can help retrieve some missing candidates back. The high-cost random disk accesses limit the number of graph traverse and candidate reranking. HM-ANN leverages the heterogeneous memory by placing pivot points promoted by the bottom-up phase in the fast memory and navigable small world graph in the slow memory without data compression. However, it will lead to more than 1.5 times larger fast memory consumption. Moreover, the slow memory is still much expensive than disk and may be not available in some platforms. The theoretical analysis of the limits and the benefits of the graph based methods are given in [35].

## 3 SPANN

In this paper, we propose SPANN, a simple but efficient vector indexing and search system, that follows the inverted index methodology. Different from previous inverted index based methods that leverage the lossy data compression to reduce the memory cost, SPANN adopts a simple memory-disk hybrid solution.

**Index structure**: The data vectors X are divided into N posting lists {X₁, X₂, ⋯, Xₙ}, X₁ ∪ X₂ ∪ ... ∪ Xₙ = X³. The centroids of these posting lists, c₁, c₂, ⋯, cₙ, are stored in the memory as the fast coarse-grained index that point to the location of the corresponding posting lists in the disk.

**Partial search**: When a query q comes, we find the K closest centroids, {c_i₁, c_i₂, ..., c_iₖ}, K ≪ N, and load the vectors in the posting lists X_i₁, X_i₂, ⋯, X_iₖ that correspond to the closest K centroids into memory for further fine-grained search.

³For convenience, we use X to denote both the matrix and the vector set.

### 3.1 Challenges

**Posting length limitation**: Since all the posting lists are stored in the disk, in order to reduce the disk accesses, we need to bound the length of each posting list so that it can be loaded into memory in only a few disk reads. This requires us to not only partition the data into a large number of posting lists but also balance the length of posting lists. This is very difficult due to the substantial high clustering cost and the balance partition problem itself. The imbalanced posting lists will lead to high variance of query latency especially when posting lists are stored in the disk.

**Boundary issue**: The nearest neighbor vectors of a query q may locate in the boundary of multiple posting lists. Since we only search a small number of relevant posting lists, some true neighbors of q that located in other posting lists will be missing. If red points are only represented by the centroid of blue posting list, they will be missing in the nearest neighbor search of yellow point.

**Diverse search difficulty**: We find that different queries may have different search difficulty. Some queries only need to be searched in one or two posting lists while some queries require to be searched in a large number of posting lists. If we search the same number of posting lists for all queries, it will result in either low recall or long latency.

All of the above challenges are the reasons why all of previous inverted index approaches adopt lossy data compression solution that stores all the compressed vectors and the posting lists in the memory.

### 3.2 Key techniques to address the challenges

In this paper, we introduce three key techniques that solve the above challenges to enable the memory-disk hybrid solution. In the index-building stage, we firstly limit the length of the posting lists to effectively reduce the number of disk accesses for each posting list in the online search. Then we improves the quality of the posting list by expanding the points in the closure of the corresponding posting lists. This increases the recall probability of the vectors located on the boundary of the posting lists. In the search stage, we propose a query-aware scheme to dynamically prune the access of unnecessary posting lists to ensure both high recall and low latency. The detail design of each technique will be introduced in the following sections.

#### 3.2.1 Posting length limitation

Limiting the length of posting lists means we need to partition the data vectors X into a large number of posting lists X₁, X₂, ⋯, Xₙ. Balancing the length of posting lists means we need to minimize the variance of the posting length Σᵢ₌₁ᴺ(|Xᵢ| − |X|/N)².

To address the posting length balance problem, we can leverage the multi-constraint balanced clustering algorithm [30] to partition the vectors evenly into multiple posting lists:

min_(H,C) ||X − HC||²_F + λ Σᵢ₌₁ᴺ (Σₗ₌₁^|X| hₗᵢ − |X|/N)², s.t. Σᵢ₌₁ᴺ hₗᵢ = 1. (1)

where C ∈ R^(N×m) is the centroids, H ∈ {0, 1}^(|X|×N) represents the cluster assignment, Σₗ₌₁^|X| hₗᵢ is the number of vectors assigned to the i-th cluster (i.e. |Xᵢ|) and λ is a trade-off hyper parameter between clustering and balance constraints.

However, we find that when the vector number |X| and the partition number N are very large, directly using multi-constraint balanced clustering algorithm cannot work due to the difficulty of large N-partition balanced clustering problem and the extremely high clustering cost. Therefore, we introduce a hierarchical multi-constraint balanced clustering technique to not only reduce the clustering time complexity from O(|X| * m * N) to O(|X| * m * k * log_k(N)) (k is a small constant) but also balance the length of posting lists. We cluster the vectors into a small number (i.e. k) of clusters iteratively until each posting list contains limit number of vectors. By using this technique, we can greatly reduce not only the length of each posting list (disk accesses) but also the index build cost.

Moreover, since the number of centroids is very large, finding the nearest posting lists for a query needs to consume large computation cost. In order to make the navigating computation more meaningful, we replace the centroid with the vector that is closest to the centroid to represent each posting list. Then the wasted navigating computation is transformed to the distance computation for a subset of real candidates.

What's more, in order to quickly find a small number of nearest posting lists for a query, we create a memory SPTAG [12] (MIT license) index for all the vectors that represent the centorids of the posting lists. SPTAG constructs space partition trees and a relative neighborhood graph as the vector index which can speedup the nearest centroids search to sub-millisecond response time.

#### 3.2.2 Posting list expansion

To deal with boundary issue, we need to increase the visibility for those vectors that are located in the boundary of the posting lists. One simple way is to assign each vector to multiple close clusters. However, it will increase the posting size significantly leading to the heavy disk reads. Therefore, we introduce a closure multi-cluster assignment solution for boundary vectors on the final clustering step – assign a vector to multiple closest clusters instead of only the closest one if the distance between the vector and these clusters are nearly the same:

x ∈ X_iⱼ ⟺ Dist(x, c_iⱼ) ≤ (1 + ε₁) × Dist(x, c_i₁),
Dist(x, c_i₁) ≤ Dist(x, c_i₂) ≤ ⋯ ≤ Dist(x, c_iₖ) (2)

This means we only duplicate the boundary vectors. For those vectors which are very close to the centroid of a cluster, they still keep one copy. By doing so, we can effectively limit the capacity increase due to closure cluster assignment while increasing the recall probability of these boundary vectors: they will be recalled if any of their closest posting lists is searched.

Since each posting list is small and we use closure assignment which will result in some posting lists that are very close to each other contain the same duplicated vectors (For example, the green vectors belong to both yellow and blue clusters). Too many duplicated vectors in the close posting lists will also waste the high-cost disk reads. Therefore, we further optimize the closure clustering assignment by using RNG rule [41] to choose multiple representative clusters for the assignment of an boundary vector in order to reduce the similarity of two close posting lists. RNG rule can be simply defined as we will skip the cluster iⱼ for vector x if Dist(c_iⱼ, x) > Dist(c_iⱼ₋₁, c_iⱼ). The insight is two close posting lists are more likely to be both recalled by the navigating index. Instead of storing similar vectors in close posting lists, it would be better to store different vectors to increase the number of seen vectors in the online search. From the vector side, it is better to be represented by posting lists located in different directions (blue and grey posting lists in the example) than just being represented by posting lists located in the same direction (blue and yellow posting lists).

#### 3.2.3 Query-aware dynamic pruning

In the index-search stage, to process different queries effectively with different resource budget, we introduce the query-aware dynamic pruning technique to reduce the number of posting lists to be searched according to the distance between query and centroids. Instead of searching closest K posting lists for all queries, we dynamically decide a posting list to be searched only if the distance between its centroid and query is almost the same as the distance between query and the closest centroid:

q --search--> X_iⱼ ⟺ Dist(q, c_iⱼ) ≤ (1 + ε₂) × Dist(q, c_i₁),
Dist(q, c_i₁) ≤ Dist(q, c_i₂) ≤ ⋯ ≤ Dist(q, c_iₖ) (3)

By further reducing those unnecessary posting lists in the closest K posting lists, we can significantly reduce the query latency while still preserving the high recall by leveraging the resource more reasonably and effectively.

## 4 Experiment

In this section we first present the experimental comparison of SPANN with the current state-of-the-art ANNS algorithms. Then we conduct the ablation studies to further analyze the contribution of each technique. Finally, we setup an experiment to demonstrate the scalability of SPANN solution in the distributed search scenario.

### 4.1 Experiment setup

We conduct all the experiments on a workstation machine with Ubuntu 16.04.6 LTS, which is equipped with two Intel Xeon 8171M CPU (2600 MHz frequency and 52 CPU cores), 128GB memory and 2.6TB SSD organized in RAID-0. The datasets we use are as follows:

1. **SIFT1M dataset** [3] is the most commonly used dataset generated from images for evaluating the performance of memory-based ANNS algorithms, which contains one million of 128-dimensional float SIFT descriptors as the base set and 10,000 query descriptors as the test set.

2. **SIFT1B dataset** [3] is a classical dataset for evaluating the performance of ANNS algorithms that support large scale vector search, which contains one billion of 128-dimensional byte vectors as the base set and 10,000 query vectors as the test set.

3. **DEEP1B dataset** [8] is a dataset learned from deep image classification model which contains one billion of 96-dimensional float vectors as the base set and 10,000 query vectors as the test set.

4. **SPACEV1B dataset** [6] (O-UDA license) is a dataset from commercial search engine which derives from production data. It represents another different content encoding – deep natural language encoding. It contains one billion of 100-dimensional byte vectors as a base set and 29,316 query vectors as the test set.

The comparison metrics to demonstrate the performance are:

1. **Recall**: We compare the R vector ids returned by ANNS with the R ground truth vector ids to calculate the recall@R. Since there exist multiple data vectors sharing the same distance with the query vector, we also replace some of the ground truth vector ids with the vector ids that sharing the same distance to the query vector in the recall calculation.

2. **Latency**: We use the query response time in milliseconds as the query latency.

3. **VQ (Vector-Query)**: The product of the number of vectors and the number of queries per second a machine can serve (which is introduced in GRIP [50]). It demonstrates the serving capacity of the search engine which takes both query latency and memory cost into consideration. The higher VQ the system has, the less resource cost it consumes. Here we use the number of vectors per KB × the number of queries per second as the VQ capacity.

### 4.2 SPANN on single machine

In this section, we demonstrate that inverted index based SPANN solution can also achieve the state-of-the-art performance in terms of recall, latency and memory cost. We first compare SPANN with the state-of-the-art billion-scale disk-based ANNS algorithms on three billion-scale datasets. Then we conduct an experiment on SIFT1M dataset to compare the VQ capacity with the start-of-the-art all-in-memory ANNS algorithms. For all the experiments in this section, we use the following hyper-parameters for SPANN: 1) use at most 8 closure replicas for each vector in the closure clustering assignment; 2) limit the max posting list size to 12KB for byte vectors and 48KB for float vectors; 3) set ε₁ for posting list expansion to 10.0, and set ε₂ for query-aware dynamic pruning with recall@1 and recall@10 to 0.6 and 7.0, respectively. We increase the maximum number of posting lists to be searched in order to get the different recall quality.

#### 4.2.1 Comparison with state-of-the-art billion-scale disk-based ANNS algorithms

We choose the state-of-the-art disk-based ANNS algorithms that can support billion-scale datasets as our comparison targets. we do not compare with HM-ANN [36] since it is not open sourced and the required PMM hardware may not be available in some platforms. Therefore, we compare SPANN only with the state-of-the-art billion-scale disk-based ANNS algorithm DiskANN. We use the default hyper parameters for DiskANN (same as the paper [39] for SIFT1B and SPACEV1B, and the pre-build index provided by [2] for DEEP1B).

We carefully adjust the navigating memory index size of SPANN by choosing suitable number of posting lists (about 10-12% of total vector number) to ensure both the algorithms consume the same amount of memory (about 32GB for SIFT1B and SPACEV1B datasets and 60GB for DEEP1B dataset). The results demonstrate the performance for SIFT1B dataset. From the results, we find SPANN significantly outperforms DiskANN in both recall@1 and recall@10 especially in the low query latency budget (less than 4ms). Especially, SPANN is more than two times faster than DiskANN to reach the 95% recall@1 and recall@10.

The performance results for SPACEV1B and DEEP1B datasets show similar patterns. It also demonstrates that SPANN outperforms DiskANN in both recall@1 and recall@10 when query latency budget is small (less than 4ms). Especially, DiskANN cannot achieve good recall quality (90%) in less than 4ms in SPACEV1B dataset, while SPANN can obtain a recall of 90% in just around 1ms. For DEEP1B dataset, SPANN can also be more than two times faster than DiskANN to reach the good recall quality (90%).

#### 4.2.2 Comparison with state-of-the-art all-in-memory ANNS algorithms

Then we conduct an experiment on SIFT1M dataset to compare the VQ capacity with the start-of-the-art all-in-memory ANNS algorithms, NSG [19], HNSW [32], SCANN [20], NGT-ONNG [23], NGT-PANNG [22] and N2 [4]. These algorithms have presented state-of-the-art performance in the ann-benchmarks [1]. We choose VQ capacity instead of latency as the comparison metric since these algorithms use much more memory to trade for low latency. However, memory is an expensive resource which has become the bottleneck for those algorithms to support large scale datasets. Therefore, we should take both memory and latency into consideration in the performance comparison. We take the SIFT1M dataset as an example due to the memory bottleneck of our test machine. We believe that the observation can be generalized to billion scale datasets.

Most of these algorithms are graph based algorithms. For NSG, we get the pre-built index from [5] and run the performance test with varying SEARCH_L from 1 to 256 which controls the quality of the search results. For HNSW (nmslib), SCANN, NGT-ONNG, NGT-PANNG and N2 we use the hyper parameters they provided in the ann-benchmarks [1] that achieve the best performance for the SIFT1M dataset.

The VQ capacity and query latency of all the algorithms on recall@1 and recall@10 show that SPANN achieves the best VQ capacity consistently across almost all the recall levels. This means although SPANN cannot achieve as low latency as the all-in-memory ANNS algorithms due to the high-cost disk accesses during the search, it can obtain the best serving capacity in the large scale vector search scenario.

#### 4.2.3 Ablation studies

In this section, we conduct a set of experiments to do the ablation studies on each of our techniques in the SIFT1M dataset.

**Hierarchical balanced clustering**: There are three fast ways to partition the vectors on a single machine into a large number of posting lists: 1) randomly choose a set of points as the posting list centroids; 2) using hierarchical KMeans clustering (HC) to select centroids; 3) using hierarchical balanced clustering (HBC) to generate a set of centroids. We compare the index quality by generating 16% points as the centroids using these three ways.

The recall and latency performance of these three centroid selection algorithms show that for both recall@1 and recall@10, we can see HBC centroid selection is better than random and HC selections, which demonstrates that balance posting length is very important for inverted index based methods. Moreover, HBC is very fast which clusters one million points into 160K clusters in only around 50 seconds with 64 threads. The whole SPANN index can be built in around 2 minutes.

Moreover, how many centroids are needed? Small number of centroids can reduce the navigating memory index size. However, large number of centroids usually means better performance. Therefore, we need to make reasonable trade-off between the memory usage and the performance. Comparing the performance of different numbers of centroids shows that the performance will increase significantly with the growth of the centroid number when the centroid number is small. However, when the number of centroids becomes large enough (16%), the performance will not increase any more. Therefore, we can choose 16% of points as the centroids to achieve both good search performance and small memory usage.

**Closure clustering assignment**: To use closure clustering assignment, we need to assign a vector to multiple closed clusters to increase its recall probability during the search. Then at most how many closure replicas we need to duplicate for a vector to ensure the performance? Too small replicas cannot help to retrieve those boundary vectors back. However, too many replicas will increase the posting size greatly which will also affect the performance.

The performance of different numbers of replicas for closure clustering assignment shows that using more than one replicas improves the performance significantly. However, when the number of replicas is larger than 8, the performance cannot be improved any more. Therefore, we choose 8 replicas for all of our experiments.

**Query-aware dynamic pruning**: In order to process different queries effectively during the online search, we introduce the query-aware dynamic pruning technique to further reduce the number of posting lists to be searched by pruning those unnecessary posting lists in the closest K posting lists.

We compare the performance with and without query-aware dynamic pruning. From the result, we can see that with query-aware dynamic pruning, we can further reduce the query latency without recall drop especially when the latency budget is small. Note that, this technique can reduce not only the query latency but also the resource usage for a query.

### 4.3 Extension of SPANN to distributed search scenario

Compared to the graph base approaches, the additional advantage for inverted index based SPANN approach is that the partial search on the nearest posting lists idea can be easily extended to the distributed search scenario, which can handle super large scale vector search with high efficiency and low serving cost. The approach Pyramid [15] demonstrates the power of balanced partition and partial search approach in the distributed scenario. To demonstrate the scalability of SPANN in distributed search scenario, we partition the data vectors X evenly into M partitions {X₁, X₂, ⋯, Xₘ} by using the multi-constraint balanced clustering and closure clustering assignment techniques in the distributed index build stage, where M is the number of machines. In the online search stage, we also adopt the query-aware dynamic pruning technique to reduce the number of dispatched machines, which effectively limits the total cpu and IO cost for a query.

The only challenge for us is that there may have some hot-spot machines. Therefore, we need to balance not only the data size but also the query access in each machine to avoid the hot spots. To address the hot-spot challenge, we partition the vectors into multiple small partitions (larger than machine number) and then use best-fit bin-packing algorithm [17] to pack the small partitions into large bins (the number of bins equals to the number of machines) according to the history query access distribution. By doing so, we can effectively balance not only the data size but also the queries processed on each machine.

We compare the optimized SPANN solution with traditional random partition and all dispatch solution to demonstrate the effectiveness of workload reduction and scalability of SPANN in distributed search scenario. We conduct the experiments below based on the SPACEV1B dataset and use about 100,000 query accesses history from production as the test workload. The workload is evenly split into three sets: train, valid and test. The train set is used in offline distributed index build, and the test set is used in the online search evaluation.

#### 4.3.1 Workload reduction and scalability

The number of vectors and the number of test query accesses in each machine when partitioning all the base vectors into 8, 16, and 32 partitions show that SPANN distributes all the data and query accesses evenly into different machines. Although it increases the number of vectors in each machine by 20% due to closure assignment, it significantly reduces the query accesses in each machine compared to the random partition solution. Moreover, SPANN can continually reduce the query accesses in each machine by using more machines while random partition cannot. This means we can always add more machines to support more queries per second, which demonstrates good scalability of our system. The reason why we can achieve good scalability is that we effectively bound the number of machines to do the search for each query.

#### 4.3.2 Analysis

Then we analyze how each technique affects the performance. We use 32 partitions case to do the ablation study. We build SPANN single machine index for each partition and use the 29,316 query vectors with ground truth as the test workload. The results demonstrate the recall, latency and the average number of machines to dispatch in the end-to-end distributed search scenario. The results show that SPANN can achieve almost the best recall in each latency budget. We can see that random partition solution needs to dispatch the query to all 32 machines for search. Using multi-constraints balanced clustering technique can significantly reduce the number of dispatched machines to 9. By adding closure assignment, we can further reduce the number of dispatched machines to 8. When all the techniques applied (including query-aware dynamic pruning in the online search), we can finally reduce the number of dispatched machines to 6.3. This means we can save about 80.3% of computation and IO cost for a query. Meanwhile, by reducing the number of machines to search for a query, we can further reduce the query latency since we reduce the number of candidates for final aggregation.

## 5 Conclusion

In this paper, we introduce SPANN, a simple but surprising efficient inverted index based ANNS system, which achieves state-of-the-art performance for large scale datasets in terms of recall, latency and memory cost. Different from previous inverted index based methods that use lossy data compression to address the memory bottleneck, SPANN adopts a simple memory-disk hybrid solution which only stores the centroids of the posting lists in the memory. We guarantee both low latency and high recall by greatly reducing the number of disk accesses and improving the quality of posting lists. Experiment results show SPANN can not only establish the new state-of-the-art performance for billion scale datasets but also achieve good scalability when extended to distributed search scenario. This demonstrates the ability of hierarchical SPANN to support super large scale vector search with high efficiency and low serving cost.

---

# SPFresh: Incremental In-Place Update for Billion-Scale Vector Search

**Yuming Xu¹,²** **Hengyu Liang¹,²** **Jin Li³,¹,²,*** **Shuotao Xu²** **Qi Chen²** **Qianxi Zhang²**
**Cheng Li¹** **Ziyue Yang²** **Fan Yang²** **Yuqing Yang²** **Peng Cheng²** **Mao Yang²**

¹University of Science and Technology of China ²Microsoft Research Asia ³Harvard University

*Work done during his final-year study at USTC and his internship at MSRA.

SOSP '23, October 23–26, 2023, Koblenz, Germany

## Abstract

Approximate Nearest Neighbor Search (ANNS) on high dimensional vector data is now widely used in various applications, including information retrieval, question answering, and recommendation. As the amount of vector data grows continuously, it becomes important to support updates to vector index, the enabling technique that allows for efficient and accurate ANNS on vectors.

Because of the curse of high dimensionality, it is often costly to identify the right neighbors of a new vector, a necessary process for index update. To amortize update costs, existing systems maintain a secondary index to accumulate updates, which are merged with the main index by globally rebuilding the entire index periodically. However, this approach has high fluctuations of search latency and accuracy, not to mention that it requires substantial resources and is extremely time-consuming to rebuild.

We introduce SPFresh, a system that supports in-place vector updates. At the heart of SPFresh is LIRE, a lightweight incremental rebalancing protocol to split vector partitions and reassign vectors in the nearby partitions to adapt to data distribution shifts. LIRE achieves low-overhead vector updates by only reassigning vectors at the boundary between partitions, where in a high-quality vector index the amount of such vectors is deemed small. With LIRE, SPFresh provides superior query latency and accuracy to solutions based on global rebuild, with only 1% of DRAM and less than 10% cores needed at the peak compared to the state-of-the-art, in a billion scale disk-based vector index with a 1% of daily vector update rate.

**CCS Concepts**: • Information systems → Information storage systems; Information retrieval.

**Keywords**: Vector Search, Incremental Update, Billion-scale

## 1 Introduction

Today deep learning models can embed almost all types of data, including speech, vision, and text information, into multi-dimensional vectors with tens or even hundreds of dimensions. Such vectors are critical for complex semantic understanding tasks [42, 49]. To enable effective vector analysis, vector nearest neighbor search (NNS) systems have become critical system components for an increasing number of online services like search [35] and recommendation [57].

To satisfy the strict query latency requirement for these online services, vector search systems often resort to approximate nearest neighbor search (ANNS) [17, 22, 34, 51, 56, 59, 65, 69], to locate as many correct results as possible (i.e., query accuracy). At the heart of a large-scale NNS system is a vector index, a key data structure that organizes high-dimensional vectors efficiently for high-accuracy low-latency vector searches [3, 9, 14, 38, 58, 67].

Like traditional indices, a high-quality vector index organizes a quick "navigation map" of vectors based on the vector proximity in a high dimensional space. The proximity measurements are often implemented with "shortcuts", which only exist between a pair of vectors with a short distance. A search query traverses the datasets based on "shortcuts" to the result set. The quality of the index for efficient traversal is highly dependent on the quality of shortcuts, where insufficient shortcuts miss relevant vectors, and extraneous shortcuts incur excessive traversal and storage costs. For high-dimensional data, vector indices require careful construction to produce a sufficient amount of high-quality "shortcuts", often as vector partitions [14, 67] or graphs of vector data [56, 65].

To add fuel to fire, there is a strong desire to support fresh update of vector indices because current systems generate a vast amount of vector data continuously in various settings. For example, 500+ hours of content are uploaded to YouTube[21] every minute, one billion new images are updated in JD.com every day [34], and 500PB fresh unstructured data are ingested to Alibaba during a shopping festival [69]. Fresh updates require vector indices to incorporate new vectors at unprecedented scale and speed while maintaining their high-quality to produce low query latency and high query accuracy of approximate vector searches.

However, it is non-trivial for vector indices to maintain high-quality "shortcuts" when updating vectors with hundreds of dimensions. Graph-based indices have inherent high cost to update vectors in place, because each insertion or removal of vector datum often requires examining the entire graph to update the edges in a high-dimensional space. One silver lining to fast vector index update is that cluster-based index, which is less costly to update than the graph-based index. Vector insertion and removal only require constant local modification to vector partitions(s). Nevertheless, as updates accumulate per vector partition, the index quality deteriorates because the data distribution skews over time, which makes partition sizes uneven and hence hurts both query latency and accuracy [34].

Because of the difficulty of running vector index updates in place, existing ANNS system support vector updates [34, 53, 59, 65] out-of-place, by periodic rebuilding of global index. A batch of vector updates are accumulated and indexed separately, i.e., out-of-place, and are periodically merged to the base index by rebuilding the entire index. Such practices introduce significant resource overheads to ANNS systems. For example, to build a global DiskANN index for a 128G SIFT dataset with a scale of 1 billion, it would require a peak memory usage of 1100GB for 2 days, or 5 days under a memory usage of 64GB with 32 vCPUs [56]. Such rebuilding can even consume more resources than the index serving costs (§2.3). In addition, such an out-of-place update method hurts the search performance of online services because it follows a Log-Structured-Merge (LSM) style for updates [53], which trades read performance for write optimization.

To scale to large vector datasets with lower costs, this paper presents SPFresh, a disk-based vector index that supports lightweight incremental in-place local updates without the need for global rebuild. SPFresh is based on the state-of-the-art cluster-based vector index design, capable of incorporating vector index updates online with low overheads while maintaining good index quality for high search performance and accuracy for billion-scale vector datasets. The core of SPFresh is LIRE, a Lightweight Incremental REbalancing protocol that accumulates small vector updates of local vector partitions, and re-balances local data distribution at a low cost. Unlike expensive global rebuilds, LIRE is capable of maintaining index quality by fixing data distribution abnormalities locally on-the-fly.

The key design rationale behind LIRE is to leverage a vector index that is already in a well-partitioned state. Small vector updates to a high-quality vector partition may only incur changes in itself and its neighboring partitions. Because the updates are small, the corresponding changes are most likely to be limited in a local region. This makes the entire rebalancing process lightweight and affordable.

Despite this opportunity, rebalancing is still non-trivial. In particular, LIRE needs to address the following challenges. 1) In order to keep search latency short, LIRE needs to maintain an even distribution of partition sizes via timely split and merge. 2) In order to keep search accuracy high, LIRE needs to identify the smallest set of vectors that cause data imbalance in the index. These vectors should be reassigned to maintain high index quality. 3) An implementation of LIRE should be lightweight with negligible performance impacts on the foreground search.

LIRE tackles these challenges by making the following four contributions:

- LIRE keeps partition size distribution uniform by splitting and merging partitions proactively and incrementally.
- LIRE formally identifies two necessary conditions for vector reassignment based on the rule of nearest neighbor posting assignment (NPA). With the necessary conditions, LIRE opportunistically identifies a minimal set of neighborhood vectors to adapt to data distribution shifts.
- An implementation of LIRE is decoupled as a two-stage feed-forward pipeline, which moves the background split-reassign off from the critical path of foreground update. Each pipeline stage is multi-threaded to saturate the high IOPS of a high-performance NVMe device.
- An SSD-backed user-space storage engine dedicated to LIRE, which bypasses legacy storage stack, prioritizes partition reads, and optimizes for partition appends.

Experiments show SPFresh outperforms state-of-the-art ANNS systems that support fresh updates on all fronts, with low and constant search/insert latency, high query accuracy, as well as efficient resource usages for billion-scale vector datasets. Instead of an additional 1000GB memory and 32 cores needed by DiskANN global rebuild, SPFresh outperforms DiskANN by 2.41× lower tail latency on average with only 10GB memory and 2 cores. Moreover, SPFresh reaches the IOPS limitation with stable performance and resource utilization. It simultaneously reaches peak 4K QPS search throughput and 2K QPS update throughput on a single NVMe SSD disk with 15 cores.

## 2 Background and Related Work

In this section, we present the basic operations in ANNS-based vector search and introduce two mainstream on-disk vector indices and their respective index update challenges.

### 2.1 Vector Search and ANNS

A common use-case of vector search involves finding the most similar items in a large dataset based on a given query. This process is often used in recommendation systems, search engines, and natural language processing tasks. To find similar images from dataset given a query image, the system first represents each image in the dataset as a high-dimensional vector through a deep learning model. The query image is also encoded into a vector in the same high-dimensional space. Then, the system calculates the similarity between the query vector and each vector in the dataset using a similarity metric, such as cosine similarity or Euclidean distance. The system ranks the images based on their similarity scores and returns the top results to the user. Essentially, the search is to find the query vector's nearest neighbors in a high-dimensional space.

Formally, given a vector set X ∈ R^(n×m) containing n m-dimensional vectors and a query vector q, vector nearest neighbor search aims to find a vector x* from X such that x* = arg min_(x∈X) Dist(x, q), where Dist is the similarity metric discussed above. This definition can be extended to K-nearest neighbor (KNN) search [67]. Modern machine learning models typically generate vectors with dimensions ranging from 100 to 10,000, or even more. For example, GPT3 generates four sizes of embedding vectors with dimensions ranging from 1024 to 12288 [48]. The high dimensionality makes it challenging to find the exact K nearest neighbors efficiently [12]. To address this issue, recent systems commonly rely on approximate nearest neighbor search (ANNS) [51, 56, 67] to make the effective trade-off across resource cost, result quality, and search latency, thus scaling to large vector datasets.

Due to its approximate nature, search result accuracy becomes an important metric to gauge the quality of a vector index. In ANNS, RecallK@K is commonly used to measure result quality. For an approximate KNN query, RecallK@K is defined as |Y∩G|/|G|, where Y is the query's result set, and G is the query's ground truth result set, |Y| = |G| = K.

### 2.2 Vector Index Organization

A vector index can be abstracted as a logical graph, where a vertex represents a vector, and an edge denotes the close proximity of two vectors in terms of distance. And vector indices for ANNS can be categorized into fine-grained graph-based vector indices and coarse-grained cluster-based vector indices. These two methods can be applied to both in-memory or on-disk scenarios.

In this paper, we only focus on on-disk vector indices since they are more cost-effective for large-scale vector-sets. Meanwhile, they pose a unique challenge for vector updates since disk writes are much more costlier than DRAM writes.

**Fine-grained graph-based vector indices** represent each vector as a vertex, and an edge exists between two vertices if they are close in distance. Locating K nearest vectors often involves best-first graph traversals, where neighboring vertices are explored in ascending distance order.

Example vector index solutions based on fine-grained graphs include neighborhood-graph based methods [15, 16, 19, 20, 23, 38, 60, 63] which organize all the vectors into a neighborhood graph with each vector connected to its nearest vectors, and hybrid methods [26, 27, 62, 68] which consist of space-partition trees and a neighborhood graph to take advantage of both tree and graph data structures. Space-partition-tree based methods [4, 6, 8, 10, 13, 18, 36, 39, 43, 45, 46, 55, 64, 72] can be treated as a special kind of fine-grained graphs. They use a tree to represent the space division and the vector to subspaces mapping. Most of these solutions are based on in-memory implementations for performance and are expensive to scale to billion-scale data-sets.

Only a few fine-grained graph-based vector indices are optimized for secondary storage (e.g., DiskANN [56] and HM-ANN [51]). Similar to external graph systems [31, 33, 52, 74], these fine-grained graph-based vector indices are stored in two parts: vertex data and edge data as vertex adjacency lists. Edge data are stored in secondary storage, and vertex data are either on disk [51] or in memory [56], where in-memory vertex data speed up computation, i.e. distance calculation in the case of ANNS [56].

To reduce search costs, DiskANN [56] employs a fixed graph traversal strategy, where it caches the neighborhood of the fixed starting point in memory to speed up graph traversal in the initial stage. DiskANN further maintains an in-memory copy of compressed vertex data (using product quantization) to speed up distance calculation during graph traversals. In contrast, HM-ANN [51] constructs a hierarchical in-memory graph where it can navigate to the nearest entry point to the main graph on secondary storage, and thus efficiently identify the target region for nearest vectors.

Although effective for vector search, graph-based vertex indices are unfriendly to updates (details in §2.3).

**Coarse-grained cluster-based vector indices** organize vector indices via clustering, where vectors in close proximity are kept in the same partition. Logically, vectors in each partition represent a fully-connected graph, while vectors across different partitions have no edge. Since no explicit edge data are required, coarse-grained cluster-based vector indices require much smaller storage. Vector search on cluster-based vector indices first identifies candidate partitions by measuring the distance to the partitions' centroids and then calculates the K nearest vectors from the candidate partitions via a full scan.

Coarse-grained cluster-based vector indices include hash-based methods [14, 24, 28, 32, 44, 50, 54, 61, 70, 71] which use multiple locality-preserved hash functions to do the vector-to-partition mapping, and quantization-based methods [5, 7, 17, 30, 73] which use Product Quantization(PQ) [29] to compress the vectors and KMeans to generate the vector-to-partition mapping codebooks.

A cluster-based vector index should preserve the balance across partitions to achieve low tail search latency. However, ANNS indices leveraging locality-sensitive hashing [14, 28, 66, 70, 71] and k-means [37] for clustering pay less attention to partition balance. Such ANNS indices often produce uneven partitions and thus are only adopted by in-memory systems where the absolute tail latency is much less pronounced than that of an on-disk solution.

SPANN [67] is the first on-disk vector index that achieves low tail search latency through balanced clustering. SPANN divides a vector-set into a large number of balanced partitions stored on disk and keeps the centroids of the partitions in the memory for quick identification of candidate partitions during search. It employs several techniques to ensure a well-balanced partition state (details in §3.1). SPANN achieves state-of-the-art performance on memory cost, result quality, and search latency across multiple billion-scale datasets.

Cluster-based vector indices are friendly to updates because each vector insertion or deletion only involves local modifications of vector data in the corresponding partition. However, a naive update on local partitions may eventually lead to imbalanced clusters and consequently deteriorate search tail latency and accuracy (more in §2.3).

### 2.3 Freshness Demands and Challenges

Modern ANNS systems are required to accommodate billions of vector updates every day while still preserving low query latency and high query accuracy. With the new popular OpenAI ChatGPT retrieval plugin [47], some AI applications built atop even require real-time updates to keep up with the updates on their personal documents or contexts, such as files, notes, emails, and chat histories, all in the form of vector, in order to retrieve most relevant snippets as new prompts. However, it is non-trivial for vector indices to maintain index quality when updating vectors.

**Out-of-place update**: For vector inserts, fine-grained graph-based indices have to connect a new vector to hundreds of neighboring vectors in order to maintain sufficient shortcuts in the high-dimensional space. Deletions of vectors are even more expensive as they often involve the total scan of a unidirectional graph.

**Table 1**: Global rebuild costs of disk-based ANNS indices for billion-scale datasets.

| System | Memory | CPU | Time |
|--------|--------|-----|------|
| DiskANN | 1100 GB | 32 cores | 2 days |
| DiskANN | 64GB | 16 cores | 5 days |
| SPANN | 260 GB | 45 cores | 4 days |

To overcome the difficulty of in-place updates, existing systems resort to out-of-place updates with periodical global updates. These systems accumulate and index delta vector updates in a separate, secondary in-memory index, which is periodically merged to the base index by a global index rebuilding process to maintain good index quality. Many popular ANNS systems, such as ADBV [69] and Milvus [65], use this method. To defer expensive global updates, Milvus even introduces multiple delta indices in memory. However, this approach requires vector search to examine both main and secondary indices, which increases resource demands and hurts search performance. Table 1 shows global rebuilds are both resource-hungry and time-consuming. For example, rebuilding a 1-billion vector index [56] for DiskANN, a recent disk-based system, needs 1100GB DRAM, 32 vCPUs, for 2 days. When limiting resources to 64GB memory and 16 vCPUs, the rebuilding time becomes significantly longer, e.g., 5 days for DiskANN. This stressful setting could also lead to a catastrophic drop in query performance because of severe computational resource starvation.

**Early attempts to in-place update**: Compared to out-of-place vector updates, few systems support in-place updates. Vearch [34] is one of such systems based on cluster-based in-memory vector indices, where it inserts a new vector to its nearest partition (a.k.a. posting, the partition is implemented as a posting list) and supports deletions by maintaining a tombstone bitmap for result filtering.

To understand the impact of Vearch's design to on-disk index, we apply Vearch's design to SPANN, the only partition-based on-disk vector index system. Results show that updating one-third of the vectors degrades the query recall by more than one point and increases tail latency by 4X, compared to static index building. The reasons are two-fold: 1) With the growth of the data size, query latency will increase due to the expansion of the posting length. 2) Since the centroids for each partition are fixed, the recall will decline as static centroids cannot capture the gradual distribution shift in the partition. To conclude, to maintain high index quality and stabilize search latency, although Vearch and the modified SPANN do not require out-of-place data structure, they still require periodical global rebuilds. For instance, Vearch performs weekly rebuilds. The rebuild overheads might be acceptable for in-memory vector indices. However, for disk-based indices like SPANN, global rebuilds are expensive, as shown in Table 1.

In summary, existing graph-based and cluster-based solutions, regardless of in-place or out-of-place, all rely on periodic global index rebuilding to preserve index quality and stabilize search performance. However, this process entails considerable resource consumption.

### 2.4 Our Goals

To this date, efficient fresh update for disk-based vector index is still an open challenge. In this paper, we aim to propose a new disk-based ANNS system to fulfill the following goals: 1) low resource cost to maintain the index for large-scale vector datasets; 2) support high throughput and low latency vector queries for both search and update; and 3) new vectors can be recalled in high probability.

To achieve this, motivated by the above understandings, we choose to follow the coarse-grained cluster-based approach to build our on-disk index, but differ from existing solutions significantly by avoiding global rebuilds completely. The proposed solution, SPFresh, performs in-place, incremental updates in the index data structure to adapt to the data distribution shift. To this end, SPFresh incorporates a Lightweight Incremental RE-balancing (LIRE) protocol, which efficiently identifies a minimal amount of partition updates introduced by new vectors for maintaining index property and thus eliminating visible accuracy loss. Equally importantly, we also address a few system challenges to make LIRE re-balance sufficiently fast and cheap to alleviate negative impacts on search latency, in particular, tail latency. Essentially, LIRE can be considered as an efficient compaction technique in the high-dimensional space.

## 3 LIRE Protocol Design

LIRE is built on SPANN [67], the state-of-the-art disk-based vector index system. In this section, we first introduce SPANN briefly and then elaborate LIRE in detail.

### 3.1 SPANN: A Balanced Cluster-based Vector Index

SPANN [67] is a billion-scale cluster-based vector index optimized for secondary storage. SPANN stores the vectors as a large number of postings* on disk, each represents a cluster of close-by vectors. Moreover, SPANN organizes a graph-based in-memory index, SPTAG [68], for the centroids of all postings, to quickly identify relevant postings for a query.

For a query, it first identifies the closest posting centroids through the in-memory index, and then loads the corresponding postings from disk to memory for further search.

To control the tail latency and maintain high search recall, SPANN makes postings well balanced by maintaining two key properties. 1) SPANN divides the vectors evenly into a large number of small-sized postings by a fast hierarchical balanced clustering algorithm, so that each query visits a similar amount of vectors for bounded search tail latency. 2) SPANN replicates a few vectors in boundaries across postings, which sufficiently maintains high search recalls.

The balanced SPANN index inspires us to propose a new lightweight incremental re-balancing (LIRE) protocol. The intuition here is a single vector update to a well-balanced index may only incur changes in a local region. This makes the entire rebalancing process lightweight and affordable.

*We use "posting" and "partition" interchangeably in this paper.

### 3.2 LIRE: Lightweight Incremental RE-balancing

A key property of a well-partitioned vector index is the nearest partition assignment (NPA): each vector should be put into the nearest posting so that it can be well represented by the posting centroid. As continuous vector updates to a posting may degrade query recalls and latency, SPFresh will split a posting after it grows to the preset maximum length. However, a naive splitting can violate the NPA property of the index.

Figure 4 illustrates a case of NPA violation. Originally, postings A and B exist with blue centroids. After posting A splits into A1 and A2 with orange centroids, a yellow vector in A2 is now closer to B's centroid (violates NPA), and a green vector in B is now closer to A2's centroid (also violates NPA).

Figure 4 illustrates a case of NPA violation. Originally, there were two postings, A and B, near each other, where the blue dots represent their centroids. At a certain point, posting A exceeds the length limit upon vector insertions and is split into two new postings, A1 and A2. The orange dots represent the new centroids of A1 and A2 elected after the split. With a naive split, the vectors in posting A will only go to Posting A1 and A2 respectively, based to their distance to new centroids. For illustrative purposes, we assume the yellow dot (a vector) goes to A2.

However, the creation of new centroids via a spit makes previous NPA-compliance obsolete for vectors in the nearby postings, A1, A2 and B. First, the nearest posting of the yellow dot changes to B, since B's centroid is closer to the yellow dot than A2's centroid. In this case, using the centroid of A2 to represent the yellow dot violates NPA. Second, the nearest posting of the green dot, which was B before the split, changes to A2. These two violations degrade the index quality and result in low recalls.

To fix the NPA violations after splits and maintain the high index quality, we design LIRE protocol, which reassigns vectors in nearby postings of a split. At its core, LIRE protocol consists of five basic operations: Insert, Delete, Merge, Split, and Reassign.

**Insert & Delete**: LIRE directly inserts a new vector to the nearest partition following the original SPANN index design. LIRE also ensures the deleted vectors will not appear in the search results and will eventually be removed from the corresponding postings.

Note that Insert and Delete are external interfaces exposed to users. The remaining three operations are internal interfaces and thus are oblivious to users. These three operations work together to keep the size of the posting small and balanced and to ensure vectors are assigned to the right posting, following the NPA property.

**Split**: When a posting exceeds a length limit, LIRE evenly splits the oversized posting into two smaller ones. As introduced in the previous section, vectors in the neighboring postings may violate the NPA property after the split. Thus, a reassign process (detailed in §3.3) will be triggered for the vectors in the split postings as well as nearby postings.

**Merge**: When a posting size is smaller than a lower threshold, LIRE identifies its nearest posting as candidates for merging. In particular, LIRE's merge process deletes one posting with its centroid (e.g., the shorter posting), and appends them to the other posting directly. After that, a reassign process is required for the vectors of the deleted posting because the deletion of their old centroid might break the NPA rule after being merged with the other posting. Reassignments will not induce splits of the merged posting because vectors can only be reassigned out. However, a reassigned vector may trigger the split to the target posting. Despite such cascading effects, §3.4 shows that LIRE's split-reassignment process will always converge.

### 3.3 Reassigning Vectors

Reassigning vectors can be expensive, because they require expensive changes to the on-disk postings for each reassigned vector. Thus it is critical to identify the right set of neighborhoods (neighboring postings) to avoid unnecessary reassignment. For a merged posting, only vectors from deleted posting require reassignment, because the deletion of a centroid does not break NPA compliance of vectors from undeleted postings.

On the other hand, a split not only deletes a centroid but creates two new ones. Therefore a split creates more complex scenarios of potential NPA violations. After examining Figure 4, we derive two necessary conditions for reassignment after splitting, assuming the high-dimensional vector space is Euclidean.

First, a vector v in the old posting with centroid A_o is required to consider being reassigned if:

D(v, A_o) ≤ D(v, A_i), ∀i ∈ {1, 2} (1)

where D denotes the distance, A_o represents the old centroid before splitting, and A_i represents any of the two new centroids. This reassignment condition means that if the old (deleted) centroid A_o is the closest centroid to the vector v, compared to new centroids (A₁ and A₂), then it cannot be ruled out the possibility that v is closer to a centroid of some nearby posting than new centroids (e.g., B in Figure 4). Note that this is a necessary condition. On the contrary, if D(v, A_o) > D(v, A_i), this shows v is having a better centroid than the old one. In this case, the neighboring certroid (e.g., B) cannot be better than the new ones, i.e., D(v, B) > D(v, A_i) based on the NPA property of D(v, B) > D(v, A_o). Thus there is no need to check reassignment in this case.

Second, a vector v in the nearby posting with centroid B needs to consider being reassigned if:

D(v, A_i) ≤ D(v, A_o), ∃i ∈ {1, 2} (2)

This is a necessary condition for a vector in posting B to be reassigned to a newly split posting with centroid A_i. Equation 2 suggests v's new neighboring centroids are getting closer (better) than the old (deleted) one. Therefore it is necessary to check if the new and closer centroids are in fact closer than v's existing centroid B (the blue dot w.r.t the green dot in Figure 4). On the other hand, if the two new centroids are farther away from any vector v outside the old posting, this means the two centroids are worse than the old one A_o, which is already farther away than v's existing centroid. In this case, there is no need to check the reassignment of v. Hence the necessary condition.

According to the two necessary conditions, a complete checking process would be extremely expensive, because it requires computing and comparing D(v, A_o) and D(v, A_i), i ∈ {1, 2} for all vectors in the dataset. To minimize the cost, LIRE only examines nearby postings for reassignment check by selecting several A_o's nearest postings, over which two condition checks were applied to generate the final reassign set. Experiments in Section 5 show empirically that only a small number of nearby postings for the two necessary condition checks is enough to maintain the index quality.

After obtaining vector candidates for reassignment, LIRE executes the reassignment. For vector candidate v, LIRE first searches v's new closest posting, then performs NPA check to get rid of false-positives: if a vector actually does not need reassignment, the reassign operation is aborted. Otherwise, LIRE appends v in the newly identified posting that is NPA-compliant and then deletes v in the original posting.

### 3.4 Split-Reassign Convergence

In this section we prove that a split-reassign action to the vector index, despite the potential of triggering cascading split-reassign actions, will converge to a final state and terminate in finite steps. We first formally define the states of vector index and the events triggering state transitions. Then we prove that state transition will converge and terminate.

**Index State**: The state of a vector index for a vector data-set V comprises of two parts.

C: set of posting centroids. (3)

M: vector membership to centroid(s). (4)

According to LIRE, given C, each vector in M is assigned to its nearest centroid in C, i.e., M is uniquely determined by C.

**Index-State Transitions**: The state transition is triggered by two types of events:

E_insert: a vector v is inserted into the vector index. (5)

E_delete: a vector v is deleted into the vector index. (6)

A reassign of vector v is considered as an E_delete of v followed by an E_insert of v. Note that an event will change the state of M, however it would not necessarily alter the state of C. Also note that only an event E_insert may incur a split action of the vector index, which alters the state of C and subsequently M. Since C uniquely determines M, we can only focus on the state change of C.

**Split-Reassign Convergence Proof**: E_delete may eventually trigger a merge during a search process (according to LIRE), and the merge obviously will terminate.

Suppose an E_insert triggers a sequence of changes of C, denoted as C_i, C_(i+1), ..., C_(i+N). To prove the convergence is to show that N is a finite number.

We note that C has the following properties:
- |C| ≤ |V|: The cardinality of C is bounded and no greater than the cardinality of V, i.e., the vector dataset.
- |C_(i+1)| = |C_i| + 1: Each split action will delete an old centroid from C_i, and adds two new centroids to it. Therefore, the cardinality of C always increases by one per split action.

Based on Property 2, |C_(i+N)| = |C_i| + N. And according to Property 1, N ≤ V − |C_i| because |C_(i+N)| ≤ V. Since |V| is finite, N must also be finite. Therefore the split action must terminate in finite steps. □

## 4 SPFresh Design and Implementation

### 4.1 Overall Architecture

Figure 5 shows the system architecture of SPFresh. SPFresh reuses the SPANN SPTAG index for fast posting centroid navigation as well as its searcher to serve queries. It further introduces three new modules to implement LIRE, namely, a light-weight In-place Updater, a low-cost Local Rebuilder, and a fast storage Block Controller.

**Updater** appends a new vector at the tail of its nearest posting and maintains a version map to keep track of vector deletion by setting a corresponding tombstone version to prevent deleted vectors from appearing in the search results. The map is also used to trace the replica of each vector. By increasing the version number, it marks the old replicas as deleted. The system keeps a global in-memory version map and stores vectors along with the version number on disk. A vector is stale if the in-memory version number is greater than that on the disk. This can be used for garbage collection caused by reassignment. The use of version can defer and batch the garbage collection so as to control the I/O overhead of vector removal. After vector insertions are completed in-place, the Updater checks the length of the posting and then sends a split job to Local Rebuilder if the length exceeds the split limit. The actual data deletions are performed asynchronously as a batch during local rebuild phase when the posting length exceeds the limit.

**Local Rebuilder** is the key component to implement LIRE. It maintains a job queue for split, merge, and reassign jobs and dispatches jobs to multiple background threads for concurrent execution.

- A split job is triggered by Updater when a posting exceeds the split limit. It cleans deleted vectors in the oversized posting and splits it into small ones if needed.
- A merge job is triggered by the Searcher if it finds some postings are smaller than a minimum length threshold. It merges nearby undersized postings into a single one.
- A reassign job is triggered by a split or merge job, which re-balances the assignment of vectors in the nearby postings.

When the background split and merge jobs are complete, SPFresh will update the memory SPTAG index with the new posting centroids to replace the old one.

**Block Controller** serves posting read, write, and append requests, as well as posting insertion and deletion operators on disk. It uses the raw block interface of SSD directly to avoid unnecessary read/write amplification incurred by some general storage engines, such as Log-structured-merge-tree-based KV store. Each posting may span multiple SSD blocks, each of which stores multiple vectors (including vector ID, version ID, and raw data). The Block Controller also maintains an in-memory mapping from the posting ID to its used SSD blocks as well as the free SSD blocks pool.

Next, we will discuss the design and implementation of Local Rebuilder (§4.2) and Block Controller (§4.3) in detail.

### 4.2 Local Rebuilder Design

In order to move split, merge, and re-assign jobs off the update critical path, SPFresh divides the update process into two parts, a foreground Updater and a background Local Rebuilder. These two components form a feed-forward pipeline, where Updater is the producer of requests to the Local Rebuilder. In this pipeline, the background Local Rebuilder is a key module that implements merge, split, and reassign operators of LIRE protocol efficiently to keep up with the foreground Updater.

#### 4.2.1 Rebuild Operators of LIRE protocol

Local Rebuilder implements LIRE with three basic operators.

**Merge**: To execute a merge job, the Local Rebuilder simply follows the merge protocol described in §3.2.

**Split**: After receiving an oversized posting split job, the Local Rebuilder first garbage collects deleted vectors in the posting and verifies whether the posting length after garbage collection still exceeds the split limit. If not, the Local Rebuilder writes the garbage-collected posting back to storage and completes the split job.

Otherwise, a balanced clustering process is triggered to split the oversized posting into two smaller ones. In particular, Local Rebuilder leverages the multi-constraint balanced clustering algorithm in [67] to generate high-quality centroids and balanced postings.

After splitting, Local Rebuilder puts two new postings back to the index and deletes the original oversized postings.

**Reassign**: A reassign job is generated by merge or split jobs. It checks if vectors in the new postings and/or their neighbors need to be relocated to re-balance the data distributions in the local region. The reassignment check is based on the two necessary conditions in §3.3. Note that neighbor posting check is not required for merge-triggered reassign.

Reassigning a vector without deleting its replicas in the unexamined postings increases the replica number. This not only increases storage overheads but also increases split and reassign frequency since the extraneous replicas take up spaces of postings. In order to efficiently identify stale vectors after reassignment without actual deletes, Local Rebuilder uses a version map to record the version number for each vector. A version number takes one byte and is stored in memory to record the version changes of a vector: seven bits for re-assign version and one bit for deletion label. When reassigning a vector, we increase its version number in the version map and append the raw vector data with its new version number to the target posting. All the old replicas with a stale version number are dropped during the search. The replicas will be garbage collected later.

#### 4.2.2 Concurrent Rebuild

In SPFresh, Local Rebuilder is multi-threaded with efficient concurrency control of updates to the in-memory and on-disk data structures. Concurrent rebuild can avoid drops of index quality due to slow re-balance.

**Concurrency Control for Append/Split/Merge**: Since append, split, and merge may update the same posting and the in-memory block mapping concurrently, We add a fine-grained posting-level write lock between these three operations to ensure a posting change is atomic.

Posting read does not require a lock. Therefore, identifying vectors for reassignment is lock-free since it only searches the index and checks the two necessary conditions. Our experiments show that even in a skewed workload, write lock contention is low, i.e., less than 1% contention cases. This is because only a small portion of postings are being edited concurrently.

During a reassign process, it is possible that a vector appends to a stale posting, which happened to be deleted concurrently. In such a case, we abort the reassignment and re-execute the reassign job for this vector. In our experiments, there are only less than 0.001% of total insertion requests encountering the posting-missing problem caused by split. As a result, the abort and re-execution overhead is minor.

**Concurrent Reassign**: SPFresh avoids concurrently reassigning the same vector at the same time. When collecting vector candidates for reassignment, Local Rebuilder gathers the current version of the candidates. Local Rebuilder atomically executes reassignment operations by leveraging atomic primitives of compare-and-swapping (CAS) for the version number. If an atomic CAS operation fails on the vector version map, reassignment is aborted since the vector becomes a stale version. Otherwise, we let the corresponding reassign proceed to the end.

### 4.3 Block Controller Design

Block Controller is a light-weight storage engine highly optimized for reading. It offers append-only operation on postings. This design takes advantage of the characteristics of postings, where old posting data is immutable, and the update introduces no additional overheads for reading (unlike a log-structured file system, where multiple additional out-of-place reads are required). It keeps appending vector updates to a posting before exceeding a length limit. When a posting exceeds the length limit, the old posting is destroyed after being split into two new ones. To avoid unnecessary overheads in file systems or other storage engines (e.g., KV store), Block Controller operates directly on raw SSD interfaces.

**SPDK-based Implementation**: Block Controller is implemented on top of SSD. It leverages the raw block interface offered by SPDK [25], a high-performance NVMe SSD library by Intel. SPDK offers a set of user-space IO libraries for accessing high-speed NVMe devices, which allow us to bypass the legacy storage stack to perform SSD I/Os directly.

**Storage Data Layout**: A Block Controller consists of in-memory Block Mapping, a Free Block Pool, and a Concurrent I/O Request Queue.

**Block Mapping** maps a posting ID to its SSD block offsets. Since the posting ID is a continuous integer, block mapping is implemented as an in-memory dense array, where each element stores the block metadata of a posting length and its SSD block offsets. A posting consists of a list of tuples in the form of <vector id, version number, raw vector>, which typically takes three to four SSD blocks. A block mapping entry only consumes 40 bytes of memory. For one billion vectors, there only exist 0.1 billion postings. In this case, block mapping only consumes about 4GB of memory.

**Free Block Pool** maintains all free SSD blocks. It keeps track of the offsets of all the free blocks to serve disk allocation, and garbage collects stale blocks after spilt and reassign.

**Concurrent I/O Request Queue** is implemented using an SPDK circular buffer, which sends asynchronous read and write to SSD device for maximized IO throughput and low I/O latency.

**Posting API & Implementation**: Block Controller provides a set of posting APIs as follows:

- **GET** retrieves posting data by the given ID. The request first looks up the block mapping to identify the corresponding SSD blocks. Asynchronous I/Os are then sent to the current I/O Request Queue. Later, all desired blocks are collected upon the completion of all I/Os.

- **ParallelGET** reads multiple postings in parallel to amortize the latency of individual GETs. This ensures fast search and update. ParallelGET allows sending a batch of I/O requests to fetch all the candidate postings, which hides the I/O latency and boosts disk utilization.

- **APPEND** adds a new vector to a posting's tail. Instead of read-modify-write at the posting-granularity, APPEND only involves read-modify-write of the last block of a posting, which reduces the amount of read/write amplification significantly. APPEND first allocates a new block, reads the original last block if the last block is not full, appends new values to the values from the last block, and then writes it as a new block. After a new block is written, it atomically updates the corresponding in-memory Block Mapping entry via a compare-and-swap operation to reflect the change. The old block will be released to the free block pool for later usage.

- **PUT** writes a new posting to SSD. Like APPEND, it allocates new blocks and writes for the entire posting blocks in bulk. Then it atomically updates the Block Mapping entry. If PUT overwrites an old posting, it releases old blocks to the Free Block Pool.

Block Controller provides a common abstraction and implementation, which can be generalized for other read-intensive applications (such as widely-used inverted index for search engine [2, 11]).

### 4.4 Crash Recovery

SPFresh adopts a simple crash recovery solution, which combines snapshot and write-ahead log (WAL). Specifically, an index snapshot is taken periodically, and all update requests between adjacent snapshots are collected into a WAL so that a crash can be recovered from the latest snapshot, followed by replaying the WAL. The WAL will be deleted when a new snapshot is generated.

To take a snapshot for a vector index, we need to record both the in-memory and on-disk data structures. For in-memory index data, we create snapshots for centroid index, version maps in Updater, and block mapping and block pool in Block Controller, and flush the snapshots to disk. Snapshots are relatively cheap because these data structures take only 40GB for billion-level dataset, which costs 2~3 seconds for a full flush on a PCI-e based NVM SSD. For disk data, thanks to our block-level copy-on-write mechanism, we can collect all the released blocks during two snapshots into a pre-release buffer, which will be added to the Free Block Pool after the next snapshot is recorded. Thus all the data blocks modified in the interim can be rolled back to be consistent with the previous snapshot. This solution saves a large amount of disk space since we only delay the space release for old blocks during two snapshots.

## 5 Evaluation

In this section, we conduct experiments to answer the following questions:

- How does SPFresh compare with state-of-the-art baselines in terms of performances, search accuracy and resource usage? (§5.2)
- What is the maximum performance of SPFresh? (§5.3)
- Can SPFresh solve the data shifting problem illustrated in Figure 2? (§5.4)
- How to properly configure SPFresh? (§5.5)

### 5.1 Experimental Setup

**Platform**: All experiments run on an Azure lsv3 [41] VM instance, which is a storage-optimized virtual machine with locally attached high-performance NVMe storage. In particular, we configured the VM with 16 vCPUs from a hyper-threaded Intel Xeon Platinum 8370C (Ice Lake) processor and 128GB memory for our experiments.

**Datasets**: We use two widely-used vector datasets to evaluate SPFresh:

- **SIFT1B** [40] is a classical image vector dataset for evaluating the performance of ANNS algorithms that support large-scale vector search. It contains one billion of 128-dimensional byte vectors as the base set and 10,000 query vectors as the test set.

- **SPACEV1B** [1] is a dataset derived from production data from commercial search engines. It represents a different form of vector encoding: deep natural language encoding. It contains one billion of 100-dimensional byte vectors as a base set and 29,316 query vectors as the test set.

**Baselines**: We compare SPFresh with two baselines:

- **DiskANN** is the state-of-the-art disk-based fresh ANNS system [53]. It is based on a graph ANNS index and uses an out-of-place update solution. We configure DiskANN with the same settings as in their paper [53]. For update configurations, DiskANN baseline processes streamingMerge, a lightweight global graph rebuild, for every new 30M vectors, where graph degree R equals 64 and insert candidate list equals 75. For search configurations, DiskANN baseline uses the default setting with beamwidth equal to 2 and search candidate list L equal to 40 for recall10@10.

- **SPANN+**, a modified version of SPANN [67] which appends updates locally to a posting without splitting and reassigning. This is an append-only version of SPFresh without the Local Rebuilder module.

**Workloads**: Three workloads are used in the experiments.

- **Workload A** simulates a realistic vector update scenario with 100 million scale of SPACEV vectors. The reason to reduce the scale from 1 billion to 100 million is that DiskANN requires several TBs of DRAM to run 1-billion scale fresh updates (shown in Table 1), which exceeds our machine's capacity. In particular, workload A simulates 1% update daily over 100 days. To generate updates realistically, we extract two disjoint SPACEV 100M datasets from SPACEV1B, where one is used as the base ANNS index data-set and the other as the update candidate pool. Each daily update epoch deletes 1% of vectors randomly from the base ANNS index, and inserts 1% of vectors randomly selected from the update data pool to the base index.

- **Workload B** has the same data scale and sampling method as Workload A but with a 100 million scale of the SIFT vector dataset.

- **Workload C** scales up our experiment data-set to be billion-scale using both the SIFT dataset and the SPACEV dataset. This workload aims at stress testing SPFresh, also with a 1% daily update rate.

**Metrics**: SPFresh is designed for online ANNS streaming scenarios. Thus, our evaluation focuses on the following four categories of metrics.

- **Search Performance**: We measure tail (P90, P95, P99, and P99.9) latency and query per second (QPS) throughput. In particular, we have a hard cut of 10ms for SPFresh and all baselines, where the system finishes the result immediately and returns the current search results.
- **Search Accuracy**: We use the percentage of ground truths recalled by SPFresh system to measure accuracy.
- **Update Performance**: insertion and deletion throughputs.
- **Resource Usages**: the memory and CPU consumption.

### 5.2 Real-World Update Simulation

In this experiment, we compare all the metrics of SPFresh with all the baselines on the real-world situation. We use Workload A and B (see §5.1) to simulate 100 days of real-world updates, and we show that SPFresh outperforms baselines in all evaluation metrics.

#### 5.2.1 System Setup

**Table 2**: Threads allocation for overall performance.

| Component | DiskANN | SPANN+ | SPFresh |
|-----------|---------|--------|---------|
| Insert | 3 | 1 | 1 |
| delete | 1 | 1 | 1 |
| Search | 2 | 2 | 2 |
| Background | 10 | 2 | 2 |
| Total | 16 | 6 | 6 |

Table 2 lists the thread allocation for each system. Specifically, we allocate threads to each system's sub-components to meet the processing requirements of handling update throughput of 600~1200 QPS. The setting of update QPS is based on Alibaba's daily update speed (100 million each day) [69]. Each system only needs one thread to serve delete requests because deletion uses tombstones to record the deletions, which is lightweight. For DiskANN, due to its high insert latency, we set its number of insert threads to 3 and the number of background merge threads to 10, because it is the minimum to keep up with the update and garbage collection process. Further increasing the number will impact the query performance in the foreground negatively. The two remaining threads are used for foreground search for DiskANN.

To be comparable with DiskANN, SPFresh and SPANN+ also set 2 search threads. Both SPANN+ and SPFresh only need one insert thread to serve 600+ QPS insert throughput and two background threads for SPDK I/O and garbage collection (or local rebuilder).

#### 5.2.2 Experiment Results

Figure 7 records a daily time series of the search tail latency, insert throughput per thread, search accuracy, and memory usage of Workload A. We can see that SPFresh achieves the best and the most stable performance on all the metrics during the 100 days.

**Low and Stable Search Tail Latency**: Figure 7 shows that SPFresh achieves low and stable tail latency in all percentile measurements. Since the overall tail latency trends are similar, we focus our discussion on the most stringent tail latency measurement, P99.9.

Experiments indicate that LIRE is able to keep posting distribution uniform because SPFresh has a stable low search P99.9 latency around 4ms. In comparision, other systems' P99.9 latency is both worse and less stable than SPFresh. DiskANN's P99.9 latency fluctuates significantly with a dramatic increase to more than 20ms during global rebuilds because a search thread could be blocked by a global rebuild even with 10ms hard latency cut. The search P99.9 latency of SPANN+ increases significantly from 4ms to more than 10ms because its posting keeps growing, inducing data skews and the increase of I/O and computation cost.

Overall, SPFresh maintains 2.41x lower P99.9 latency to DiskANN on average and expands its latency advantage to SPANN+. The low and stable search tail latency can be attributed to the LIRE protocol. During the experiment, we found that only 0.4% insertion will cause rebalancing. Among them, the average split number is 2, and the maximum split number is 160, with a cascading length of 3. The merge frequency is only 0.1% of the update (insertion and deletion). On average, each time 5094 vectors are evaluated, and only 79 are actually reassigned.

**High Search Accuracy**: SPFresh achieves a higher search accuracy compared to baselines. Both SPANN+ and SPFresh would not violate the NPA of a cluster-based index. Therefore, the accuracy of SPANN+ and SPFresh grows gradually since newly inserted vectors are all assigned to a subset of postings due to the data shifting. Therefore, queries to these new vectors can easily hit since search and insertion follow the same search path to get the nearest postings.

Although SPANN+ search accuracy increases in a similar trend like SPFresh, the gap in accuracy increases over time. The increasing gap between these two systems is because the index quality of SPANN+ degrades as partition distributions skew over time.

DiskANN proposes an algorithm to reduce the overhead of global rebuild by eliminating outdated edges from all vertices and populating edges for a new vertex using the neighborhood of its deleted neighbors. This method aims to reduce the decline in accuracy due to a decreased number of edges caused by vector deletions without reconstructing its graph-based index completely. However, experiments show that such a method cannot prevent DiskANN's search accuracy from decreasing over time.

**High and Stable Update Performance**: SPFresh achieves 1.5ms average insert latency and stable tail latency. On the other hand, DiskANN suffers from the heavy computation caused by in-memory graph traversal, and thus results in higher latency and lower throughput. Compared to SPANN+, we can see that SPFresh's lightweight Local Rebuilder will not affect the foreground insert performance.

**Low Resource Utilization**: For resource usage, SPFresh achieves as low as 5.30X lower memory usage than baselines during the whole update process. DiskANN occupies an extra 60G memories for background streamingMerge and 15GB for the second in-memory index for the update. SPANN+ needs much larger block-mapping entries to allow a larger posting length. SPFresh keeps the memory under 20GB, which only grows slightly over time because new metadata are created for each new posting triggered by splits. SPFresh also maintains a reasonable disk size. In the index, we find that 86% of the total vectors have more than one replica, and on average, one vector has 5.47 replicas, which is similar to the index built statically.

We also ran the same experiment on Workload B and reached a similar conclusion with DiskANN. Note that SPANN+ achieves similar performance with SPFresh on the SIFT dataset, which is almost uniformly distributed. This is expected because background garbage collection should be able to prune stale vectors on SPANN+ without splits on uniform data-set. Consequently, SPANN+ achieves a similar index quality as that of SPFresh because its posting distribution does not shift much.

### 5.3 Billion-Scale Stress Test

We scale up vector data size to billion-level and configure the system to show the best performance SPFresh achieves with the given resource. We use Workload C (see §5.1) to simulate a 20-day real-world update scenario. We demonstrate that SPFresh has fully saturated SSD's bandwidth and performed well with stable resource utilization.

#### 5.3.1 System Setup

**Table 3**: Thread allocation for SPFresh in billion-scale tests.

| Component | #threads | Component | #threads |
|-----------|----------|-----------|----------|
| Delete/Re-insert | 4 | Search | 8 |
| Background | 3 | Total | 15 |

Table 3 lists thread allocation for SPFresh's stress tests. To fully achieve the IOPS of SSD, our setting maximizes search throughput while supporting maximum update throughput.

Azure lsv3 has a max guaranteed NVMe IOPS of 400K [41]. We first run an experiment to find out the max search throughput lsv3 can handle. The IOPS and search throughput almost reach the peak at 8 search threads on a single SSD disk.

When search thread count is set to 8 for max search throughput, a fore-ground thread count of 4 saturates the update throughput. Therefore we set the thread counts as in Table 3 for this experiment.

#### 5.3.2 Experiment Results

Figure 9 records a daily time series of the search P99.9 latency on both uniform and skew datasets, search/insert throughput, and the IOPS of WorkLoad C. We can see that SPFresh reaches the IOPS limitation with stable performance and resource utilization throughout the entire run.

**High NVMe SSD IOPS Utilization**: As we can see from Figure 9, SPFresh always fully utilizes the NVMe's bandwidth, even exceeding the max guaranteed IOPS of Azure-lsv3. Thanks to LIRE's lightweight protocol, we can see SPFresh's bottleneck is in the disk IOPS, before reaching the CPU and memory resource limit.

**Stable Search and Update Performance**: As the data scale grows from 100M to 1B, the search latency is stable, just like that in §5.2. There is some slight increase of P99.9 latency in the beginning when the first split jobs are triggered. In this case, the P99.9 latency increases slightly because of the gradual growth of in-memory index size, which makes the in-memory computation more costly over time.

**Stable Accuracy**: During the entire stress test, the accuracy of SPFresh remains stable, which is higher than 0.862 for the uniform dataset and 0.807 for the skew dataset by searching the nearest 64 postings.

### 5.4 Data Distribution Shifting Micro-benchmark

In this experiment, we replay the experiment in §2.3 to demonstrate that LIRE is required to re-balance the shifting data distribution. We compare four systems in this experiment, where Static is our target since it has no updates. For the rest of the three systems, we start with a naive system with in-place update only, i.e., SPANN+, and gradually add sub-components of LIRE into the system.

**Experiment Results**: The recall and latency trade-off result shows that with a relaxed search latency, the figure shows that recall improves for all four systems. Meanwhile, as the curve moves northwest, the system shows a higher ANNS index quality with a more accurate recall and a lower latency. An in-place update-only solution may have a high recall but at the expense of high latency. Adding a split component into the in-place update decreases the search latency with the same accuracy. Adding the reassignment component further decreases the search latency. The performance of SPFresh with in-place update + split/reassign is the closest one to the Static's results, which represent the ideal cases.

### 5.5 Parameter Study

In this experiment, we investigate the proper parameter configurations for SPFresh to achieve maximum performance. Experiment results show that the Reassignment only requires checking a limited scope of proximate postings for scanning to attain a good index quality. Furthermore, SPFresh demands a minimal increase in computational resources, specifically in terms of threads, to accomplish high throughput while also demonstrating good scalability.

**Reassign Range**: The first parameter we examine is the reassign range, i.e., the size of the local rebuild range. Reassign range is measured by the number of nearby postings to check for vector reassignment after a new posting list is created. In this experiment, we use the same setting as in §2.3.

We vary reassign range from the nearest 0, i.e., only process reassign in the split posting, to the nearest 128 postings. As the reassign range increases, accuracy also increases with the same search time budget because more NPA-violating vectors are identified and reassigned. The accuracy increase rate wanes off as the reassign range increases, where there is only a marginal increase from range 64 to 128. Consequently, we chose 64 as SPFresh's default reassign range.

**Fore/Back-ground Update Resource Balance**: The foreground In-place Updater and background Local Rebuilder work as a feed-forward pipeline as detailed in §4.2. In this experiment, we examine the proper resource ratio for In-place Updater and Local Rebuilder to make their processing speed balanced in the pipeline. Specifically, we configure the foreground and background threads and measure the throughput to see when the update resource is balanced.

As the left part of Figure 12 shows, a single-threaded background Local Rebuilder can keep up the foreground In-place Updater until foreground threads are set to 2. Similarly, as on the right side of Figure 12, an 8-threaded foreground In-place Updater needs at least four threads for foreground In-place Updater to generate enough requests for the Local Rebuilder. Based on the result, SPFresh sets a thread ratio of 2:1 between the foreground In-place Updater and the background Local Rebuilder balances the feed-forward pipeline.

## 6 Conclusion

SPFresh supports incremental in-place update for billion-scale vector search. It implements LIRE, a Lightweight Incremental RE-balancing protocol to split overly large postings and reassign vectors across neighboring postings when necessary. Experiments show that SPFresh can incorporate continuous updates faster with significantly lower resources than existing solutions while maintaining high search recalls by (1) LIRE identifies a minimal set of neighborhood vectors in the large index space for updating to adapt to data distribution shift; (2) the index re-balancing operations and the foreground queries are decoupled, and handled by efficient concurrency control mechanisms, avoiding operation interference. SPFresh's solid single-node performance builds a strong foundation for the future distributed version.

## Acknowledgments

We thank all the anonymous reviewers for their insightful feedback, and our shepherd, Nitin Agrawal, for his guidance during the preparation of our camera-ready submission. This work is supported in part by the National Natural Science Foundation of China under Grant No.: 62141216, 62172382 and 61832011, and the University Synergy Innovation Program of Anhui Province under Grant No.: GXXT-2022-045. Cheng Li and Qi Chen are the corresponding authors.
