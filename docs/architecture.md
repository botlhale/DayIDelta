# Architecture

## Overview
DayIDelta manages SCD2 history for event-based time series data using a surrogate `dim_day` table instead of relying on event timestamps. This ensures deterministic batch lineage, multi-source safety, and clean querying.

## Key Components
| Component | Responsibility |
|-----------|----------------|
| dim_day | Stores monotonically increasing surrogate day_id per batch load day |
| Observation Table | Holds user key columns + tracked columns + start_day_id + end_day_id |
| DayIDelta Function | Orchestrates insert/close operations per batch |
| Environment Helpers | Platform-specific setup (e.g., Unity Catalog configuration) |
| Chatbot Module | Generates SQL/PySpark queries from natural language |

## Flow
1. Ensure current batch has (or creates) a `day_id` (dim_day insert if new date).
2. Identify new/changed rows by comparing tracked column values across active versions.
3. Close replaced versions by setting `end_day_id` to current batch day_id.
4. Insert new active versions with `start_day_id = current_day_id` and `end_day_id = 0`.
5. Source-aware expiration: only consider rows tied to sources present in the batch.

## Source-Aware Expiration
Without this feature, missing rows from one source might close records from another. DayIDelta scopes expiration to the set of sources (or key partitions) observed in the batch.

## Deterministic Behavior
- No reliance on event-time ordering.
- Re-running a batch with identical content should not create duplicate versions (idempotent logic can be enforced by pre-checks or hashing tracked columns).

## Schema Simplicity
Only these system columns are added:
- start_day_id
- end_day_id

## Chatbot Integration
The chatbot is intentionally separated (pure code generation) and does not mutate data. It references table/column metadata via user-supplied schema objects.

## Extensibility
To add a new platform:
- Implement environment setup helper.
- Provide docs and example notebooks.
- Add tests verifying table naming semantics.

## Future Ideas
- Optional streaming interface / micro-batch ingestion.
- Partitioning strategies and cost-based optimization hints.
- Physical plan inspection utilities.
