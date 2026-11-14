# ODS

Parsed rows from raw files, one model per source, append-only. Every row carries `valid_time`, `source_time`, `ingested_at`, `known_at`, `load_type`, `batch_id`, `source_file` and the parser version. Values keep the source's own units and slot labels. Never updated in place (F3 tests this).
