# ingest

Capture: everything that touches the sources. Parsing lives in [parse/](../parse/).

| Module | Holds |
| --- | --- |
| `sources.py` | The source registry: one `Source` per capture job, with its items per mode, retry policy and pacing |
| `fetch.py` | `jtf fetch`: download, store unchanged with a sidecar, ping the source's health check |
| `landed.py` | Which files have landed since a watermark, for the Dagster raw-bucket sensor |

**Modes.** `daily` always downloads, so each day records what was there. `initial` loads history once: items come from name patterns and date ranges, anything already stored is skipped, so a stopped load resumes. `recapture` asks with `If-Modified-Since` (TEPCO ignores `If-None-Match`) and stores only files that changed. Open-Meteo history is paced from its `X-RateLimit-Cost` header and stops cleanly at a call budget or a 429; history-only sources (`openmeteo_previous_runs`, `openmeteo_single_runs`) have no daily schedule.

**Fetch** runs only in the capture jobs, outside Dagster. It downloads a file and stores it unchanged through `storage/`, with a metadata sidecar: fetch time (the evidence for `known_at`), URL, request parameters and response headers, `Last-Modified` among them. It never parses: the update time some files carry inside is read later by the parser, as `source_time`. One narrow exception: the FIT/FIP disclosure links its files by unguessable ids, so `meti_fit_capacity`'s summary page is an index item (`FetchItem.expand`). It is stored like any file, then read for links only; a linked release is fetched once, and a page that links nothing fails the capture. Its initial load also takes the page's archived copies from the Wayback Machine, which date the older releases.

**Legacy raw prefixes.** On 2026-11-15, before capture moved to one job per source, test runs wrote a few files under `tepco_sameday/` (now `tepco_area_forecast`, `tepco_area_bg_plan`, `tepco_area_actuals_today`, `tepco_demand_outlook`), `tepco_forecast_next_day/` (now `tepco_area_forecast_next_day`, `tepco_area_bg_plan_next_day`) and `openmeteo_live/…_forecast_jma.json` (now `forecast.json`). Raw files are never deleted. ODS is built from the source registry, so these prefixes are never parsed.

Be polite to the sources: about 10 requests a day in normal running, no polling loops. TEPCO and JEPX files are Shift_JIS.
