# e2e tests

Only the guarantees no single component can prove alone. Everything else is tested by its owner, so this suite stays small.

| Test | Proves | Components |
| --- | --- | --- |
| Determinism | Replaying the same `T` twice gives identical output | jtf + dbt |
| Future poison | Rows with `known_at` after `T` and absurd values change nothing in a replay at `T`, so nothing bypasses `as_of` | jtf + dbt |
| Backfill stability | A late history load, raw files for past periods fetched now, leaves every decision time's inputs as they were, except rule-dated rows before their source's capture start | jtf + dbt + the ODS fingerprint |
| Reproduce | A what-if run, then a reproduce of that run, match exactly | jtf + dbt + run records |
| One day end to end | A small fixture world goes raw → ODS → DWD → DWS → forecast with the expected shape and checks | all of the pipeline |

Run against a throwaway PR dataset in the dev project, through Workload Identity Federation in CI. They run when `jtf/` or `warehouse/` changes.

"Identical" is checked by fingerprint, the function the ODS check uses ([fingerprint.py](../jtf/src/jtf/parse/fingerprint.py)): each decision time's DWS and ADS rows, and each run's inputs and outputs (`ads_runs`).

## The test world

A window of real raw files, cut from the raw bucket by a script and loaded through the parsers into a CI prefix, then built by dbt with the window's variables (`first_target_day`, `sun_first_day`, `sun_last_day`). It is target days 15 to 19 November 2026, decided at 09:00 JST on the 14th to the 18th: the real seam at capture start.

**What the spike found** (2026-11-18, the files uncut, in a scratch prefix):

| Question | Found |
| --- | --- |
| Which files | 42 raw files and their sidecars, 11.4 MB: the November month file of TEPCO's actuals as captured each day and re-captured (10), Previous Runs' two November files, 12 Single Runs, the 5 live captures, and for capacity the four releases a decision time in the window reads (8 spreadsheets), one archived copy of the page and the page's own fetches |
| Capacity's dates | The four releases get the same `known_at` as in dev from one archived copy of the page (27 October 2026), which shows the latest announcement of all four |
| Cutting | The city spreadsheets are 7 MB of the 11.4 and give 1.32 M of the 1.44 M rows, but the warehouse uses 20 Shizuoka cities of them, and their parser checks only for the national total. Cut to each sheet's header rows, the national total and Shizuoka, the world should be about 4.5 MB |
| Time | Parsing 172 s, almost all of it the uncut city spreadsheets; a full `dbt build` 78 s; rebuilding the models 16 s |
| What fails on five days | 242 of 248 green. `dim_sun_slots__matches_spa` fails with the sun limited to November (its reference has days all year): the e2e build skips that test or keeps the sun's full range. The outcome model needs the predictions table, which Python writes: the e2e setup makes it empty first, as `jtf ci create` does |
| Complete | Every decision time has its 48 slots, D−2 in all of them, capacity, clear sky and weather |
| Covered | The seam as in dev, to the version: 423 captured and 2,076 rule-dated versions of the actuals, Previous Runs' 104 corrections, the weather served from Previous Runs, then a Single Run, then live. Not covered: a late history load after capture start (the backfill test plants one) and a captured capacity release (none exists yet) |
| Determinism | A rebuild gave identical fingerprints for all 21 tables |
| Cost | About 4 GB billed for parsing, two builds, every test and the fingerprints, most of it BigQuery's 10 MB minimum on each of some 300 small queries; a CI run is a parse and a build |

