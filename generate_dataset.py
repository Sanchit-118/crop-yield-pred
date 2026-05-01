from __future__ import annotations

import csv
import random
from pathlib import Path


CROPS = {
    "Rice": {"rainfall_mm": 1120, "temperature_c": 28, "soil_ph": 6.1, "yield_base": 6.7},
    "Wheat": {"rainfall_mm": 620, "temperature_c": 21, "soil_ph": 6.7, "yield_base": 4.9},
    "Maize": {"rainfall_mm": 760, "temperature_c": 25, "soil_ph": 6.3, "yield_base": 5.4},
    "Cotton": {"rainfall_mm": 680, "temperature_c": 29, "soil_ph": 6.8, "yield_base": 3.4},
    "Sugarcane": {"rainfall_mm": 1240, "temperature_c": 30, "soil_ph": 6.6, "yield_base": 8.8},
}

SOILS = ["Loamy", "Clay", "Sandy", "Black", "Alluvial"]
REGIONS = ["North", "South", "East", "West", "Central"]


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _row_for(crop_type: str) -> list[object]:
    profile = CROPS[crop_type]
    rainfall = random.gauss(profile["rainfall_mm"], 150)
    temperature = random.gauss(profile["temperature_c"], 3.4)
    humidity = random.uniform(48, 92)
    soil_ph = random.gauss(profile["soil_ph"], 0.45)
    nitrogen = random.uniform(40, 165)
    phosphorus = random.uniform(20, 110)
    potassium = random.uniform(30, 140)
    pest_risk = random.uniform(1, 9.5)

    rainfall_penalty = abs(rainfall - profile["rainfall_mm"]) / 350
    temp_penalty = abs(temperature - profile["temperature_c"]) / 10
    ph_penalty = abs(soil_ph - profile["soil_ph"]) / 1.6
    nutrient_bonus = (nitrogen + phosphorus + potassium) / 350
    humidity_bonus = humidity / 100
    pest_penalty = pest_risk / 12

    estimated_yield = (
        profile["yield_base"]
        + nutrient_bonus * 1.25
        + humidity_bonus * 0.4
        - rainfall_penalty * 1.1
        - temp_penalty * 0.9
        - ph_penalty * 0.7
        - pest_penalty * 1.1
        + random.gauss(0, 0.22)
    )
    estimated_yield = _clamp(estimated_yield, 1.1, 11.8)

    return [
        crop_type,
        random.choice(REGIONS),
        random.choice(SOILS),
        round(rainfall, 2),
        round(temperature, 2),
        round(humidity, 2),
        round(soil_ph, 2),
        round(nitrogen, 2),
        round(phosphorus, 2),
        round(potassium, 2),
        round(pest_risk, 2),
        round(estimated_yield, 2),
    ]


def ensure_dataset(path: Path) -> None:
    if path.exists():
        return

    random.seed(42)
    path.parent.mkdir(parents=True, exist_ok=True)

    headers = [
        "crop_type",
        "region",
        "soil_type",
        "rainfall_mm",
        "temperature_c",
        "humidity_pct",
        "soil_ph",
        "nitrogen_kg_ha",
        "phosphorus_kg_ha",
        "potassium_kg_ha",
        "pest_risk",
        "yield_ton_per_hectare",
    ]

    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for crop_type in CROPS:
            for _ in range(120):
                writer.writerow(_row_for(crop_type))


if __name__ == "__main__":
    ensure_dataset(Path("data") / "crop_yield_dataset.csv")
