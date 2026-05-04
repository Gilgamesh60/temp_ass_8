# Screenshots

Drop the required evidence images here before submitting:

- `spark_master_ui.png` — `http://<master>:8080` showing **2 active workers**
- `spark_app_ui.png` — `http://<master>:4040` showing the job DAG / stages during a run
- `ray_dashboard.png` — `http://<head>:8265` cluster resource view (2 nodes listed)
- `ray_jobs.png` (optional) — Ray Jobs tab during a run

Tips:
- To catch the Spark application UI (4040), the driver must still be alive. `spark_clean.py` respects the `SPARK_HOLD_SECONDS` env var — run it like `SPARK_HOLD_SECONDS=180 python spark_clean.py ...` and screenshot within those 3 minutes.
- Same trick for Ray: `RAY_HOLD_SECONDS=180 python ray_clean.py ...`.
