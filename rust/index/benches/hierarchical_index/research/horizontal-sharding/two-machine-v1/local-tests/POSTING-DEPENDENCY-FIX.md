# Transfer dependency diagnosis

Transfers now depend on the representative vectors used to produce posting codes. These representative vectors are called centroids. An unrelated posting update can change a leaf’s structural revision without changing its centroid or placement. The owner validates current source membership and destination absence while it protects both affected endpoints. Structural splits retain their broad revision checks.

A deterministic endpoint test reproduces the defect in 0.01 seconds: prepare codes for one vector, append unrelated IDs to both leaves, then attempt the original transfer. A fixture with 4,096 vectors and 32 dimensions also reproduces the exact 100K failure with 32 workers, a split threshold of 128, a merge threshold of 32, and a branching factor of 100. Its shallow tree keeps most leaves in the same neighbor search.

The fixed source passes 144 native tests. Both endpoints still reject changed centroids, placement, node kind and membership. A failed owner also cancels queued, unedited transfer protection promptly rather than waiting for the reservation deadline. The complete settings, work counts, red and green logs, source hashes and native CLI artifacts appear in [POSTING-DEPENDENCY-FIX.json](POSTING-DEPENDENCY-FIX.json).

The clustering kernel and thread schedule are not deterministic, so the red and green runs perform different amounts of work. Their elapsed times are not a matched speedup. Actual 100K and 3M measurements remain necessary. Large node-ID values also do not establish tree size because node allocation and operation attempts share a counter.
