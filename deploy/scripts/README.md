# scripts

`promote.sh` will point the capture job, the fallback job and the Dagster code location at an image digest. A release is a pointer change, never a rebuild; rollback is the same script with the previous digest.
