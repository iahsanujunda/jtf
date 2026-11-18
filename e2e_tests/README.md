# e2e tests

Only the guarantees no single component can prove alone. Everything else is tested by its owner, so this suite stays small.

| Test | Proves | Components |
| --- | --- | --- |
| Determinism | Replaying the same `T` twice gives identical output | jtf + dbt |
| Future poison | Rows with `known_at` after `T` and absurd values change nothing in a replay at `T`, so nothing bypasses `as_of` | jtf + dbt |
| Backfill stability | A late history load, raw files for past periods fetched now, leaves every decision time's inputs as they were, except rule-dated rows before their source's capture start | jtf + dbt + the ODS fingerprint |
| Reproduce | A what-if run, then a reproduce of that run, match exactly | jtf + dbt + run records |
| One day end to end | A small fixture world goes raw → ODS → DWD → DWS → forecast with the expected shape and checks | all of the pipeline |

"Identical" is checked by fingerprint, the function the ODS check uses ([fingerprint.py](../jtf/src/jtf/parse/fingerprint.py)): each decision time's DWS rows, every table after a rebuild, and each run's inputs and outputs (`ads_runs`).

## Running them

`make e2e` with the settings in `.env`, and CI's `e2e` job on every pull request that changes `jtf/`, `warehouse/dbt/` or `e2e_tests/` ([workflows](../.github/workflows/README.md)). Both work in throwaway datasets of the dev project, `ci_<run id>[_<attempt>]_e2e_<layer>`, made and dropped by the tests; their tables expire after a day in case a run dies first. `make test` doesn't run them.

The tests share one world, built once per session ([conftest.py](conftest.py)): its datasets made as `jtf ci create` makes them, its raw files parsed with every parser side by side, then `dbt seed` and `dbt run` with the window's variables under the `e2e` target, which fails a query reading over 1 GB. [harness.py](harness.py) holds what the tests do with it: parse, run dbt, plant files and fingerprint. A run takes about five minutes: a minute for the world, the rest the tests. It bills about 5.5 GB for 0.3 GB read: some 450 queries, each billed BigQuery's 10 MB minimum, half of them the data tests.

| Test | Plants | Compares |
| --- | --- | --- |
| One day end to end | Nothing: a what-if run of target days 17 and 18, its outcomes built, then every data test | Every decision time has 48 slots with D−2, capacity and clear sky; 48 outcomes a day, the 17th all labelled; the run counts the 16th approximate and the 17th exact, as `dws_fidelity` has them |
| Determinism | Nothing: `dbt run --full-refresh`, every model from the ODS again | Every table's fingerprint before and after; two what-if runs of the window, their inputs and outputs |
| Reproduce | Nothing | A what-if run of the window and its reproduce, inputs and outputs |
| Future poison | Copies of the newest actuals file, live forecast and Previous Runs file with every figure 999,999, captured an hour after the last decision time | What each decision time sees, before and after; the poison is in DWD, so a pass means `as_of` kept it out; a reproduce of a run made before |
| Backfill stability | October's actuals as the initial load fetched them, a month the world has no file of, and the newest November file again with one slot's solar 1,000 MW higher, both loaded as an initial load the day after the window | What each decision time sees; the actuals versions read at seven times through the `as_of` predicate, two before the window and its five: none read before is gone, every new one is rule-dated and before capture start; the correction is known only from the late load; the ODS rows loaded before are as loaded; a reproduce of a run made before |

Each test was seen to fail with the logic it guards removed (2026-11-18): fidelity counting rule-dated versions as exact, a model reading the clock, a reproduce comparing run IDs, D−2 read as its newest version instead of through `as_of`, and corrections dated by the history rule.

## The test world

A window of real raw files in [world/raw](world/raw), made from the raw bucket by [world/make_world.py](world/make_world.py) and loaded through the parsers into a CI prefix, then built by dbt with the window's variables (`first_target_day`, `sun_first_day`, `sun_last_day`). It is target days 15 to 19 November 2026, decided at 09:00 JST on the 14th to the 18th: the real seam at capture start.

**What the spike found** (2026-11-18, the files uncut, in a scratch prefix):

| Question | Found |
| --- | --- |
| Which files | 42 raw files and their sidecars, 11.4 MB: the November month file of TEPCO's actuals as captured each day and re-captured (10), Previous Runs' two November files, 12 Single Runs, the 5 live captures, and for capacity the four releases a decision time in the window reads (8 spreadsheets), one archived copy of the page and the page's own fetches |
| Capacity's dates | The four releases get the same `known_at` as in dev from one archived copy of the page (27 October 2026), which shows the latest announcement of all four |
| Cutting | The city spreadsheets are 7 MB of the 11.4 and give 1.32 M of the 1.44 M rows, but the warehouse uses 20 Shizuoka cities of them, and their parser checks only for the national total. Cut to each sheet's header rows, the national total and Shizuoka, each is 38 kB instead of 1.78 MB, and the world is 4.5 MB. Its sidecars give the cut file's size and hash, and name the original's |
| The cut world | Byte for byte the same on a second run of the script (the spreadsheets' save times are fixed). Built, it gives the uncut world's tables, 20 of 21 by fingerprint; the 21st, capacity's figures per file, carries the time of the parse, and equals dev's on every other column |
| Time | Parsing 135 s, cut or uncut: BigQuery's round trips, a few per file, not the spreadsheets, so the harness parses sources side by side; a full `dbt build` 78 to 80 s; rebuilding the models 16 s |
| What fails on five days | 242 of 248 green. `dim_sun_slots__matches_spa` fails with the sun limited to November (its reference has days all year): the e2e build skips that test or keeps the sun's full range. The outcome model needs the predictions table, which Python writes: the e2e setup makes it empty first, as `jtf ci create` does |
| Complete | Every decision time has its 48 slots, D−2 in all of them, capacity, clear sky and weather |
| Covered | The seam as in dev, to the version: 423 captured and 2,076 rule-dated versions of the actuals, Previous Runs' 104 corrections, the weather served from Previous Runs, then a Single Run, then live. Not covered: a late history load after capture start (the backfill test plants one) and a captured capacity release (none exists yet) |
| Determinism | A rebuild gave identical fingerprints for all 21 tables |
| Cost | About 4 GB billed for parsing, two builds, every test and the fingerprints, most of it BigQuery's 10 MB minimum on each of some 300 small queries; a CI run is a parse and a build |

Two parses of the same world differ in one column, the parse time `dwd_capacity_area_files` keeps; two builds of one parse are identical. So the tests compare builds, never parses.
