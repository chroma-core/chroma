# Finer balancing jobs

The scheduler distributes individual leaves that need balancing through a shared queue. Each idle worker takes the next leaf, so a concentration of work under one parent no longer reserves that entire subtree for one worker. The queue prioritizes larger postings, whose splits usually require more computation.

A round begins after the preceding workers join and queued transfers drain. The writer snapshots eligible leaf IDs and starts at most the configured number of workers. A relaxed atomic counter assigns each snapshot slot once. The existing balancing function rechecks each leaf's current type and size before attempting its claim. Structural changes retain the existing tree lock and parent recovery; explicit leaf claims retain ownership of posting membership and node lifetime. Job scheduling does not reserve ancestors.

An earlier job may retire a queued leaf or change its size, in which case the live check skips obsolete work. New split children and destinations that become oversized are discovered at the next round boundary. The existing eight-round cap, pending transfer commit gate, navigation refresh, and writer poison behavior remain in force. A residual size violation is valid under the existing cap policy; pending ownership transfers still block commit.

The source change replaces coarse subtree roots and static assignment with leaf jobs; it does not alter transfer execution. This candidate is directly based on eea4ada6f21554adb535d1ffdea95f74cb76650b to keep its benchmark independent of the batching experiment. The diagnostic field named `partitions` counts leaf jobs in this candidate, so its counts must not be compared as though both candidates measured subtree roots.

The expected benefit is less time spent waiting for a worker assigned a large subtree. Possible costs are more simultaneous neighboring-leaf conflicts, additional retries, and an extra round for newly created children that a delayed subtree traversal could otherwise discover. Benchmarks must measure those tradeoffs rather than assuming the tail reduction is a net gain.
