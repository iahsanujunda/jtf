# faults

Fault switches that are off in normal runs, and scenario presets used with `--fault=F…` on the same commands.

Each preset records its expected outcome before it runs: what should catch it, how fast, and how it should be handled. Results are written up in [docs/scenarios](../../../../docs/scenarios/).

| Scenario | Phase |
| --- | --- |
| F2 lookahead leak, F1 late weather | 1 |
| F3–F13 | 2–5, see the PRD's fault table |
