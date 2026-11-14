# ADS

Published outputs. Each forecast run stores `T`, run ID, image digest, model versions, fallback level, fidelity and its input snapshot, so a replay can be compared with the original feature by feature. Replays and shadow runs write to their own datasets, never to the production forecast table.
