# Experiment 29 construction outlier

The first physical 3M attempt completes the first million vectors and inserts the second million, but its second balance does not finish before the operator stops it. This attempt supplies no completed save, reader result or end-to-end performance measurement. Its receipts remain in `operations/window-epoch29-measure`.

The first-million insertion takes 3.384 seconds and balancing takes 13.752 seconds. Corrected experiment 28 takes 3.306 and 15.894 seconds respectively. Before the second balance, the attempts have 679 and 692 leaves and approximately 1.260 and 1.268 billion cumulative distance calculations. These initial measurements do not identify the later cause.

A ten-second profile late in the second balance contains 501 samples without sample loss. Distance calculations account for 42.74%, local navigation expansion for 16.41%, sorting for 6.33% and splitting for 4.66%. This establishes active computation during the sample. It does not distinguish a large individual job from repeated structural work or establish the initial degree of parallelism.

Independent archive comparison finds no construction algorithm difference between corrected experiment 28 and experiment 29. The new storage cache and publication maps are empty until saving. Storage connections and reuse begin after construction.

The shared construction scheduler contains an inherited deadline defect. Idle workers can return after the 300-second limit without setting shared failure or cancellation. Active jobs and workers that keep dequeuing work bypass that deadline check, and scoped joins wait for active workers. This can amplify a long balance by reducing its available workers. It does not establish the original cause of this attempt's long balance. A repeat with unchanged source is a separate observation; retain this incomplete attempt alongside it.

The operator stops the owned coordinator after profiling. Its exit receipt records signal 15, confirmed cleanup and 449.700 seconds of wall time. Both task machines are subsequently API-stopped. Covered compute for this window is $1.626281061553955. The real S3 adapter test passes before this workload and remains valid separate evidence for immutable writes, multipart replay and conditional publication.
