# storage

Raw and model files addressed by URL, so the rest of the code never imports a cloud SDK.

- `gs://` backend for GCP, `file://` backend for unit tests and offline work
- Raw writes are create-only (`if_generation_match=0` on GCS, exclusive create on disk): an existing file can never be overwritten
- Planned raw layout: `raw/{source}/{extraction_date}/{fetched_at}_{filename}`, with a `.meta.json` sidecar next to each file
- Model files and the production pointer file live under `models/`; the pointer is written only by CI
