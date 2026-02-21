# MLOps Mini Pipeline

A reproducible batch pipeline that ingests cryptocurrency OHLCV data, computes a rolling-mean signal, and emits structured JSON metrics — fully containerised with Docker.

---

## Project Structure

```
.
├── run.py            # Main pipeline script
├── config.yaml       # Pipeline configuration
├── data.csv          # Input OHLCV dataset (10,000 rows)
├── requirements.txt  # Python dependencies
├── Dockerfile        # Container definition
├── metrics.json      # Example output metrics
├── run.log           # Example execution log
└── README.md         # This file
```

---

## Dependencies

| Package  | Version |
|----------|---------|
| pandas   | 2.0.3   |
| numpy    | 1.24.4  |
| pyyaml   | 6.0.1   |

---

## Setup Instructions

```bash
# Install dependencies
pip install -r requirements.txt
```

---

## Local Execution

```bash
# Run the pipeline locally
python run.py --input data.csv --config config.yaml \
              --output metrics.json --log-file run.log
```

---

## Docker Instructions

```bash
# Build the Docker image
docker build -t mlops-task .

# Run the container (prints metrics to stdout, exits 0 on success)
docker run --rm mlops-task
```

---

## Configuration (`config.yaml`)

```yaml
seed: 42      # Random seed for reproducibility
window: 5     # Rolling mean window size
version: "v1" # Pipeline version tag
```

---

## Expected Output

`metrics.json` will be written with the following structure:

```json
{
  "version": "v1",
  "rows_processed": 10000,
  "metric": "signal_rate",
  "value": 0.4987,
  "latency_ms": 19,
  "seed": 42,
  "status": "success"
}
```

If an error occurs:

```json
{
  "version": "v1",
  "status": "error",
  "error_message": "Description of what went wrong"
}
```

---

## Pipeline Logic

1. **Config Loading** — Reads `config.yaml`, sets NumPy random seed.
2. **Data Ingestion** — Loads and validates the CSV; checks for the required `close` column.
3. **Rolling Mean** — Computes a rolling mean on `close` with the configured window (`min_periods=1` handles the initial rows).
4. **Signal Generation** — Assigns `1` where `close > rolling_mean`, else `0`.
5. **Metrics Emission** — Writes JSON to `--output` and prints to stdout; logs everything to `--log-file`.

---

## Error Handling

The pipeline gracefully handles:
- Missing or unreadable input file
- Empty CSV file
- Missing `close` column
- Invalid or malformed YAML config
- Any unexpected runtime exception

All errors are captured in the JSON output with `"status": "error"` and a descriptive `error_message`, and the process exits with a non-zero code.
