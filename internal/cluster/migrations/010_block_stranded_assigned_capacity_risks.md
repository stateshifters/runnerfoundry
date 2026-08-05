# Migration 010 risk register

Migration 010 adds constant-default aggregate statistic columns and one nullable
blocked-state column. PostgreSQL takes brief `ACCESS EXCLUSIVE` locks for the
table alterations and index replacement. RunnerFoundry already requires a
stopped-cluster rollout at this compatibility boundary, so application queries
cannot queue behind those locks.

There is no backfill: existing observations receive zero statistics and existing
demands remain unblocked. The runnable index is rebuilt transactionally as a
partial index because only unblocked obligations are schedulable.

Down is lossless when no blocked obligations remain. It refuses rollback while
blocked state exists because schema generation 9 cannot preserve that safety
state. Resolve the upstream assignment state with the generation-10 binary,
confirm no blocked obligations remain, and then retry the rollback.
