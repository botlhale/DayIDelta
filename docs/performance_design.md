# Performance & Design Rationale

## Goals
- Deterministic SCD2 lineage for time series
- Minimize accidental version churn
- Support partial (multi-source) ingestion
- Keep schema lean

## Avoiding MERGE
MERGE can introduce:
- Schema evolution race conditions
- Hidden complexity around matched/unmatched semantics
- Unintended updates due to broad conditions

DayIDelta uses explicit:
1. Identify active records impacted.
2. Close them (set end_day_id).
3. Insert new versions.

## Complexity Considerations
Let:
- N = existing active records for involved sources
- B = size of the new batch
Operations aim for O(B + subset(N)) scanning rather than full table scans (depending on partitioning and pruning).

## Index / Partitioning
Strategies:
- Partition rarely on high-cardinality keys (may cause small-file explosion).
- Consider clustering (ZORDER in some platforms) by key columns and/or start_day_id.
- Use file compaction (OPTIMIZE) periodically.

## Storage Efficiency
Only two system columns:
- start_day_id
- end_day_id (0 for active)
No need for valid_from/valid_to timestamps—simplifies filtering.

## Multi-Source Expiration
Expiration limited to sources present in current batch mitigates unintentional closure from partial upstream failures.

## Idempotency
Replays with identical batch content should not produce new versions if logic includes:
- Pre-check hashing (future enhancement)
- Compare tracked columns precisely

## Potential Enhancements
- Optional bloom filters / indexing for faster key lookups
- Data skipping metrics instrumentation
- Pluggable “conflict resolution” for overlapping keys in same batch

## Benchmarking Guidance
Metrics to track:
- Number of closed vs inserted rows
- Mean/median file size
- Query latency for common patterns (current, point-in-time, comparison)