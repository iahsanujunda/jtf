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

On 2026-11-20, in [notebook 13](../../notebooks/13_what_a_leak_looks_like.ipynb), with the code of commit `7471b38`. No image exists yet, so there is no digest.

- **The lag leak:** a query that reads the same slot's output on D−7 to D−1 from `dwd_tepco_actuals` with `known_until is null`, the latest version, in place of the as-of filter. Its seven-day mean replaced the honest one in the model's inputs
- **The weather leak:** a query that builds the area's forecast from `dwd_weather_slots` at one day's lead and at none, with no filter, as `dws_weather_area` builds it at two days'. Its radiation, cloud cover and temperature replaced the honest ones
- **Replayed:** the 16 test months of the honest backtest, March 2025 to June 2026, each month's model trained by the pipeline's own code on the leaked rows

Of the 177,128 values the lag query read for the scored slots, 19,836 (11.2%) were not known at their decision time, all from D−1. Of the one-day-old forecast's daylight slots, 6.4% were known at the decision time; of the same day's, none.

## What caught it, and how fast

| Leak | Caught by | How fast |
| --- | --- | --- |
| Lags, read around the filter | The rule that every read of a versioned table in a feature query is filtered by the decision time (`warehouse/dbt/rules/test_as_of.py`). Run on the leak's own query, it reports a read filtered by nothing | In CI's warehouse job, about three minutes after a pull request is opened, before any data is read. The merge is blocked |
| Weather, read around the filter | The same rule, the same way | The same |
| Weather, read through the filter | Nothing to catch: the fresher forecast is not there. Every daylight slot from 06:00 on would have no weather, and the test that each daylight slot has weather at the decision time fails | On the first build of the feature table |
| Either, had it reached a build | The end-to-end test that plants absurd values known just after a decision time and requires that nothing at that time changes | In CI's end-to-end job, 8 to 14 minutes |

**What did not catch it: the score.** The plan lists the gap between a leaky and an honest backtest as a detector. For the lag leak there was no gap to see.

## Accuracy cost

How much better each leak made the backtest look, in mean absolute error over capacity on daylight slots. The honest model misses by 5.21%.

| Leak | Expected | Found | Months better, of 16 | Cloudy days (honest 7.26%) |
| --- | --- | --- | --- | --- |
| Lags, the model | Under 0.2 points | 0.03 points: 5.18% | 7 | 7.19% |
| Lags, persistence (12.64%) | 1 to 2 points | 1.23 points: 11.41% | – | – |
| Lags, the seven-day mean (10.52%) | A few tenths | 0.25 points: 10.27% | – | – |
| Weather one day old | 0.3 to 0.8 points | 0.87 points: 4.34% | 15 | 5.43% |
| The same day's weather | 1 to 2 points | 2.26 points: 2.95% | 16 | 2.96% |
| Both | – | 2.26 points: 2.95% | 16 | 2.97% |

A backtest that had read the same day's weather would have reported a model that misses by 2.95%. Live, the same model misses by 5.2%.

## Fix

None was needed: the leaks never entered the pipeline.

Had one been merged, the repair would be to put the filter back, rebuild the feature table, and run every backtest made since again. Which runs those are can be read from `ads_ml_runs`: a run records a fingerprint of the features it read, so a run made on leaked features has another fingerprint than the same run on honest ones.

## Regression test added

The form of both leaks, a join on `known_until is null` alone, is now one of the cases the rule's own test tells apart (`warehouse/dbt/rules/test_as_of.py`, "the latest version, as a label is read"). The rule already refused it; the case holds the rule to that if it is ever rewritten.

## What surprised me

- **A leak can be invisible in the score.** One value in nine that the lag leak read came from the future, and the model's backtest moved by 0.03 points, better in 7 months of 16. The model barely uses the past week, so it barely used what leaked. A comparison of scores would have passed this leak; only the check of the query caught it. The size of a leak in the data and its size in the metric are different things
- **The weather leak was larger than predicted:** 2.26 points for the same day's forecast, where 1 to 2 were expected. On cloudy days the error fell from 7.26% to 2.96%, level with clear days
- **That figure is also a map of what is left.** Nearly all of the model's remaining error on cloudy days is the error of a forecast two days old. A forecast one day fresher is worth 0.9 points, which is about what the live capture offers honestly
- **A correction made weeks later can't be measured on history.** Every future value the lag leak read was from D−1: the history holds one version of each past actual. The first monthly re-capture, on 1 December, brings the first real corrections
