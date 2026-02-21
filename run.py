import argparse
import json
import logging
import os
import sys
import time

import numpy as np
import pandas as pd
import yaml


# Argument parsing

def parse_args():
    parser = argparse.ArgumentParser(description="MLOps Mini Pipeline")
    parser.add_argument("--input",    required=True, help="Path to input CSV file")
    parser.add_argument("--config",   required=True, help="Path to YAML config file")
    parser.add_argument("--output",   required=True, help="Path to output metrics JSON file")
    parser.add_argument("--log-file", required=True, dest="log_file", help="Path to log file")
    return parser.parse_args()


# Logging setup

def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("mlops_pipeline")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# Config loading

def load_config(config_path: str, logger: logging.Logger) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML config: {e}")

    # Validate required keys
    required_keys = {"seed", "window", "version"}
    missing = required_keys - set(config.keys())
    if missing:
        raise ValueError(f"Config is missing required keys: {missing}")

    # Type validation
    if not isinstance(config["seed"], int):
        raise ValueError("Config 'seed' must be an integer")
    if not isinstance(config["window"], int) or config["window"] < 1:
        raise ValueError("Config 'window' must be a positive integer")
    if not isinstance(config["version"], str):
        raise ValueError("Config 'version' must be a string")

    logger.info(f"Config loaded: seed={config['seed']}, window={config['window']}, version={config['version']}")
    return config


# Data ingestion

def load_data(input_path: str, logger: logging.Logger) -> pd.DataFrame:
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if not os.access(input_path, os.R_OK):
        raise PermissionError(f"Input file is not readable: {input_path}")

    try:
        df = pd.read_csv(input_path)
    except pd.errors.EmptyDataError:
        raise ValueError("Input CSV file is empty")
    except pd.errors.ParserError as e:
        raise ValueError(f"Invalid CSV format: {e}")

    if df.empty:
        raise ValueError("Input CSV file contains no data rows")

    if "close" not in df.columns:
        raise ValueError(f"Required column 'close' not found. Available columns: {list(df.columns)}")

    logger.info(f"Data loaded: {len(df)} rows")
    return df


# Processing

def compute_rolling_mean(df: pd.DataFrame, window: int, logger: logging.Logger) -> pd.Series:
    rolling_mean = df["close"].rolling(window=window, min_periods=1).mean()
    logger.info(f"Rolling mean calculated with window={window}")
    return rolling_mean


def generate_signals(df: pd.DataFrame, rolling_mean: pd.Series, logger: logging.Logger) -> pd.Series:
    signals = (df["close"] > rolling_mean).astype(int)
    logger.info("Signals generated")
    return signals

def write_metrics(output_path: str, payload: dict):
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
    print(json.dumps(payload, indent=2))


# Main

def main():
    start_time = time.time()

    args = parse_args()

    logger = setup_logging(args.log_file)
    logger.info("Job started")

    version = "v1"  # fallback default before config is loaded

    try:
        # 1. Load config
        config = load_config(args.config, logger)
        seed    = config["seed"]
        window  = config["window"]
        version = config["version"]

        # Set random seed for reproducibility
        np.random.seed(seed)

        # 2. Load data
        df = load_data(args.input, logger)

        # 3. Rolling mean
        rolling_mean = compute_rolling_mean(df, window, logger)

        # 4. Signal generation
        signals = generate_signals(df, rolling_mean, logger)

        # 5. Metrics calculation
        rows_processed = len(df)
        signal_rate    = round(float(signals.mean()), 4)
        latency_ms     = int((time.time() - start_time) * 1000)

        logger.info(f"Metrics: signal_rate={signal_rate}, rows_processed={rows_processed}")
        logger.info(f"Job completed successfully in {latency_ms}ms")

        metrics = {
            "version":        version,
            "rows_processed": rows_processed,
            "metric":         "signal_rate",
            "value":          signal_rate,
            "latency_ms":     latency_ms,
            "seed":           seed,
            "status":         "success",
        }

        write_metrics(args.output, metrics)
        sys.exit(0)

    except (FileNotFoundError, PermissionError, ValueError) as e:
        logger.error(str(e))
        error_payload = {
            "version":       version,
            "status":        "error",
            "error_message": str(e),
        }
        write_metrics(args.output, error_payload)
        sys.exit(1)

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        error_payload = {
            "version":       version,
            "status":        "error",
            "error_message": f"Unexpected error: {str(e)}",
        }
        write_metrics(args.output, error_payload)
        sys.exit(1)


if __name__ == "__main__":
    main()
