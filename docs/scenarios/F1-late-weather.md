# F1: late weather

A forecast for day D is made at 09:00 on D−1 from the weather forecast captured at 08:45 that morning. This scenario takes that capture away: at 09:00 the day's weather is not there.

Unlike a leak, this fault removes information, and it will happen: a capture job fails, or the provider is down. The pipeline has to make a forecast all the same, and say what it was made from.

## Expected outcome

Written on 2026-11-20, before anything was run.

**What the pipeline does today**, read from its code and data, not yet tried:

- **Nothing older can stand in.** A morning capture reaches to 07:30 two days ahead, so yesterday's stops before the target day's daylight. The plan's fallback level 1, an older forecast that covers the day, can't be served by the capture as it is set
- **The feature table has no weather for that day,** and its test that every daylight slot has a forecast fails
- **A run over that day still gives a forecast,** made from empty weather inputs, and nothing on the forecast says so

**What should catch it**

- The freshness check on the weather capture, due by 09:00, and the capture job's own health check: within minutes of the missed capture
- The feature table's test, on the first build after it

**How it should be handled** (to be built here): the forecast falls back to the plan's level 2, the seven-day mean with its learned range. Each prediction records its level, and the run counts the decision times it degraded.

**What it should cost**, on March 2025 to June 2026, where the model misses by 5.21% of capacity and its range holds 73%:

- **Level 2, the seven-day mean with its learned range:** about 10.4%, twice the error, with the range holding about 79%. That is that baseline's own recorded backtest (10.36%, 78.7%), and a run with every day degraded should reproduce it
- **The model given empty weather instead:** worse than level 2 and badly calibrated. It was never trained on a row without weather, so it has no reason to behave
- **A model trained without the day's weather:** about 10.1%, the figure notebook 11 found for that set of inputs, a little better than level 2 and not worth a second model to keep
- **Level 1,** had an older forecast been there: not measured here. A forecast one day staler cost 0.9 to 1.4 points in the leak scenario's mirror image

## What was injected

On 2026-11-20, with the code of commit `e9033ff`. No image exists yet, so there is no digest.

- **Before anything was built**, in [notebook 14](../../notebooks/14_a_forecast_without_the_days_weather.ipynb): the day's weather emptied in every row to be predicted, and four ways of forecasting without it scored on the 16 test months, March 2025 to June 2026
- **After the fallback was built**, through the pipeline's own assets against throwaway datasets holding copies of dev's tables:
  - **The preset `fault=F1`** on a what-if run of the model over the same 16 months: the day's weather emptied at every decision time
  - **One late morning planted in the feature table:** the five weather columns of 15 June 2026 emptied, and a what-if run over 14 to 16 June

Dev's tables were not touched.

## What caught it, and how fast

| What | Catches | How fast |
| --- | --- | --- |
| The capture job's health check and Dagster's freshness check on the weather capture, both due by 09:00 | The capture itself not arriving | Minutes after the missed capture. Not triggered here: no capture was really missed |
| The feature table's test that every daylight slot has a forecast | The day having no weather | On the first build. On the planted morning it failed with 30 daylight slots |
| The run itself | A day asked for without its weather | In the run: it answered 15 June from the fallback, marked its 48 slots level 2, and recorded 1 of 3 decision times degraded |

Before this scenario the third row did not exist: the run would have handed the model empty weather.

## Accuracy cost

Mean absolute error over capacity on daylight slots, March 2025 to June 2026, every day's weather late.

| Forecast made from | Expected | Found | Range holds |
| --- | --- | --- | --- |
| The day's weather (level 0) | – | 5.21% | 73% |
| **The seven-day mean with its learned range (level 2, built)** | About 10.4%, range about 79% | **10.36%** | **78.7%** |
| The model, given empty weather (what a run did before) | Worse than level 2, badly calibrated | 18.82%, 17 points too low | 14% |
| A model trained without the day's weather | About 10.1% | 10.14% | 75%, and 53% on cloudy days |

A late capture doubles the error and keeps the range honest. The faulted run through the pipeline gave the fallback model's own forecasts on all 23,376 slots, scored 10.36% with a range holding 78.7%, and a reproduce of it matched.

## Fix

Built in this scenario:

- **A model that reads the day's weather names a fallback.** For the LightGBM model it is the seven-day mean with its learned range; for the scaled seven-day mean, the plain seven-day mean
- **A day whose weather is missing in any daylight slot is answered whole by the fallback.** A morning with half its weather is a late morning
- **Every prediction records its level** (0 or 2), and it reaches the outcomes. **Every run records how many decision times it degraded**, and Dagster shows it
- **`fault=F1` is a preset on the replay job,** and a reproduce of a faulted run carries the fault

Not built: level 1, an older forecast that still covers the day. The morning capture reaches to 07:30 two days ahead, so yesterday's stops before the target day's daylight. Asking the provider for one more day would give level 1 something to be made from.

## Regression test added

- Unit tests of the run: a late day falls back whole and says so; a night slot alone doesn't make a day late; the fault degrades every day to the fallback's own forecasts; a reproduce carries the fault; a model with no fallback refuses; a model that reads no weather is never degraded
- The outcomes' unit test holds the level through to the scored table

Each was seen to fail with its logic removed: 13 breaks of the run, one of the outcomes.

## What surprised me

- **The model reads a missing forecast as a dark day.** Given empty weather it said 16% of capacity at noon, day after day, where 51% came. A tree sends a missing value down whichever side fitted its training rows better, and no training row lacked its weather, so that side was never learned. It was not a little worse than the fallback, as expected, but far worse than any of them: 18.8%, with a range that held one outcome in seven
- **And it would have done that silently.** The feature table's test fails on such a day, but a run asked for it still returned a forecast, with nothing on it to say what it was made from. The gap was not the missing fallback so much as the missing word
- **The plan's level 1 has nothing to be made from.** It was written as "the previous run that covers D". With a capture that reaches two days ahead to 07:30, no previous capture covers D's daylight
