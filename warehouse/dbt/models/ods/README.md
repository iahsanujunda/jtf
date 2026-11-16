# ODS

Not built by dbt. The jtf parsers ([jtf/src/jtf/parse](../../../../jtf/src/jtf/parse/)) load ODS, one table per file layout, append-only; dbt reads the tables as sources ([_ods__sources.yml](_ods__sources.yml)), named `ods/<table>` like their Dagster assets so lineage runs through. Every row carries its lineage: `source_file`, `extraction_date`, `fetched_at`, `load_type`, `source_time`, `parser_version`, `ingested_at` and `batch_id`. ODS has no `known_at` and no slot times: DWD derives both. Values keep the source's own units and slot labels. Never updated in place (F3 tests this).
