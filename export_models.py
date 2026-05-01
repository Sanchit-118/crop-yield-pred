from __future__ import annotations

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = BASE_DIR / ".packages"
if PACKAGE_DIR.exists():
    sys.path.insert(0, str(PACKAGE_DIR))

import joblib

from app import ensure_dataset, DATASET_PATH, evaluate_models
import pandas as pd


def main() -> None:
    ensure_dataset(DATASET_PATH)
    dataframe = pd.read_csv(DATASET_PATH)
    evaluation = evaluate_models(dataframe)

    models_dir = BASE_DIR / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    for result in evaluation["results"]:
        filename = result.name.lower().replace(" ", "_") + ".pkl"
        joblib.dump(result.pipeline, models_dir / filename)

    joblib.dump(evaluation["best_model"].pipeline, models_dir / "best_model.pkl")
    print("Model files exported to:", models_dir)


if __name__ == "__main__":
    main()
