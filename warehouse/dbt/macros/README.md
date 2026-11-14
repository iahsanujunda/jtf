# macros

| Macro (planned) | Does |
| --- | --- |
| `as_of(relation, T)` | The latest version of each row with `known_at ≤ T`. The only way DWS may read inputs |
| Unit conversions | kWh per slot ÷ 500 → MW; 万kW × 10 → MW |
| Slot normalisation | Slot codes 1–48, `0000`–`2400` and start-time labels → slot start in JST |
