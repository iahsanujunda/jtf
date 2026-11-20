# F2: lookahead leak

A forecast for day D is made at 09:00 on D−1 and may read only what was known then. This scenario lets it read more, on purpose and in two ways, to see what a leak would have looked like in the backtest and what stands in its way.

The leaks were never put into the pipeline. They were built in a notebook's own queries ([notebook 13](../../notebooks/13_what_a_leak_looks_like.ipynb)), and the model was trained and scored on them with the pipeline's code: dev's tables never held a leaked feature.

## Expected outcome

Written on 2026-11-20, before anything was run.

**The two leaks**

| Leak | What the features read instead |
| --- | --- |
| Lags | The same slot's output over D−7 to D−1 at its latest version, where the pipeline reads D−8 to D−2 as known at 09:00 on D−1. D−1 is not over at 09:00, and later corrections are included |
| Weather | The forecast one day old, and the same-day one, where the pipeline reads the two-day-old forecast: the only one issued before 09:00 on D−1 for every slot of D |

**How much better the backtest should look**, on March 2025 to June 2026, where the honest model misses by 5.21% of capacity:

- **Lags, the model:** less than 0.2 points better. The model barely uses the past week: leaving it out costs 0.06 points ([notebook 11](../../notebooks/11_inputs_of_the_solar_model.ipynb), section 4.2)
- **Lags, the baselines:** much more. Persistence reads one day back instead of two, which should be worth 1 to 2 points of its 12.6; the seven-day mean a few tenths of its 10.5
- **Weather one day old:** 0.3 to 0.8 points better
- **Weather of the same day:** 1 to 2 points better, most of it on cloudy days, where the two-day-old forecast is weakest (7.3%)

**What should catch it**

- **The lag leak** skips the as-of filter. The rule that parses every feature query fails on it in CI, within minutes of a pull request, and the merge is blocked. If it got past that, the end-to-end test that plants absurd values known just after the decision time would fail
- **The weather leak, read through the as-of filter,** is no leak: the fresher forecast is simply not there for the midday slots, and the test that every daylight slot has weather fails
- **The weather leak, read around the filter,** is caught by the same rule as the lag leak
- **A forecast dated too early** is the case no rule over SQL can see: the filter lets it through in good faith. It is held by the tests of each source's dating rule and by the delays having been measured, and what remains is reported, not removed: every run says how many of its decision times rest on a rule

**Handling:** block the merge.

## What was injected

After the run.

## What caught it, and how fast

After the run.

## Accuracy cost

After the run.

## Fix

After the run.

## Regression test added

After the run.

## What surprised me

After the run.
