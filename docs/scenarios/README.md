# Fault scenarios

One write-up per scenario from the PRD's fault table, named `F<n>-<short-name>.md`. Phase 1 covers F2 (lookahead leak) and F1 (late weather).

The expected outcome is written **before** the scenario runs; the rest after.

## Template

- **Expected outcome:** what should catch it, how fast, and how it should be handled
- **What was injected:** the switch, the dates replayed, the image digest
- **What caught it, and how fast**
- **Accuracy cost:** the metric with and without the fault
- **Fix**
- **Regression test added**
- **What surprised me**
