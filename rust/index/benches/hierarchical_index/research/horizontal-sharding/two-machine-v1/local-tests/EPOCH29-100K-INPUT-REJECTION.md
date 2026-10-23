# Experiment 29 smaller validation admission

The 100K physical validation attempt exits before construction because the workload requires the canonical input to contain exactly the requested number of rows. The retained file contains 3M rows. This attempt produces no construction, save or reader measurement.

The peer exit receipt confirms cleanup and exit code 1 with the error `canonical input must contain exactly the workload rows`. The coordinator never starts. Both task instances are API-stopped. Receipts remain in `operations/window-epoch29-storage100k`; the spend ledger includes this window once. The next full 3M attempt uses the retained input at its matching size and includes the same first-save and unchanged-repeat-save validation.
