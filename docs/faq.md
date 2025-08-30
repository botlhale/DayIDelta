# FAQ

## Why a separate dim_day table?
Ensures monotonic surrogate IDs independent of event timestamps.

## Why not use event time directly?
Event-time can arrive late/out-of-order, breaking deterministic lineage.

## How are records expired?
By setting end_day_id for previously active versions whose keys + tracked column values differ or whose key disappeared within the batch’s source scope.

## Can I run multiple sources in parallel?
Yes—each source batch triggers selective expiration only within that source’s partition key set (assuming source column included in key).

## How do I query the current dataset?
Filter where end_day_id = 0.

## How do I query as-of a prior day_id?
`start_day_id <= X AND (end_day_id = 0 OR end_day_id > X)`

## How does the chatbot know my schema?
You provide a TableSchema object with key and tracked columns.

## What if I add a new tracked column later?
You may need a backfill step generating new versions or a one-time migration script.

## Can I delete old versions?
You can’t safely remove historical rows without losing lineage. Consider retention policy if compliance allows.

## What about time travel vs day_id?
Delta time travel is orthogonal (transaction log-based). day_id expresses business batch lineage, not commit order.

## Is streaming supported?
Not yet—roadmap includes a micro-batch wrapper.

## How do I ensure performance on very large tables?
Periodic OPTIMIZE, pruning by partition predicates, and limiting tracked columns.