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
