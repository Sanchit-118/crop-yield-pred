from __future__ import annotations

import json
import os
import smtplib
import sqlite3
import sys
import warnings
from email.mime.text import MIMEText
from email.message import EmailMessage
from io import StringIO
from datetime import datetime, timedelta
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = BASE_DIR / ".packages"
if PACKAGE_DIR.exists() and os.getenv("USE_LOCAL_PACKAGES") == "1":
    sys.path.append(str(PACKAGE_DIR))

from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from flask import Response
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None
try:
    from apscheduler.schedulers.background import BackgroundScheduler
except ImportError:
    BackgroundScheduler = None
from werkzeug.security import check_password_hash, generate_password_hash
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from generate_dataset import ensure_dataset


def load_local_env_file() -> None:
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


load_local_env_file()


app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "crop-yield-demo-secret-key")

DATASET_PATH = BASE_DIR / "data" / "crop_yield_dataset.csv"
CUSTOM_DATASET_PATH = BASE_DIR / "data" / "custom_crop_yield_dataset.csv"
DATABASE_URL = os.getenv("DATABASE_URL")
LOCAL_DB_PATH = BASE_DIR / "data" / "local_app.db"
DB_MODE = os.getenv("APP_DB_MODE", "auto").strip().lower()
IS_RENDER = bool(os.getenv("RENDER")) or bool(os.getenv("RENDER_EXTERNAL_URL"))
DEFAULT_DB_BACKEND = (
    "postgres"
    if DATABASE_URL and psycopg2 is not None and (DB_MODE == "postgres" or (DB_MODE == "auto" and IS_RENDER))
    else "sqlite"
)
ACTIVE_DB_BACKEND = DEFAULT_DB_BACKEND
MODEL_EVAL_PROFILE = os.getenv("MODEL_EVAL_PROFILE", "auto").strip().lower()
LIGHTWEIGHT_MODEL_EVAL = MODEL_EVAL_PROFILE == "light" or (
    MODEL_EVAL_PROFILE == "auto" and IS_RENDER
)
FEATURE_COLUMNS = [
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
]
CATEGORICAL_COLUMNS = ["crop_type", "region", "soil_type"]
NUMERICAL_COLUMNS = [column for column in FEATURE_COLUMNS if column not in CATEGORICAL_COLUMNS]
MAX_VISIBLE_ALERTS = 4
MEANINGFUL_ALERT_TYPES = {"best_crop_changed", "yield_improvement", "risk_increase", "season_shift"}

MODEL_EVAL_SETTINGS = {
    "baseline_ann_max_iter": 240 if LIGHTWEIGHT_MODEL_EVAL else 900,
    "ga_ann_max_iter": 240 if LIGHTWEIGHT_MODEL_EVAL else 900,
    "ga_search_iterations": 2 if LIGHTWEIGHT_MODEL_EVAL else 5,
    "ga_search_cv": 2 if LIGHTWEIGHT_MODEL_EVAL else 3,
    "rf_estimators": 72 if LIGHTWEIGHT_MODEL_EVAL else 220,
    "rf_max_depth": 8 if LIGHTWEIGHT_MODEL_EVAL else 12,
    "gb_estimators": 120 if LIGHTWEIGHT_MODEL_EVAL else 240,
    "gb_learning_rate": 0.06 if LIGHTWEIGHT_MODEL_EVAL else 0.05,
}

RISK_WEIGHTS = {
    "rainfall_deviation": 0.30,
    "temperature_stress": 0.25,
    "soil_condition": 0.25,
    "pest_risk": 0.20,
}

IDEAL_GROWTH = {
    "Rice": {"rainfall_mm": 1120, "temperature_c": 28, "soil_ph": 6.1},
    "Wheat": {"rainfall_mm": 620, "temperature_c": 21, "soil_ph": 6.7},
    "Maize": {"rainfall_mm": 760, "temperature_c": 25, "soil_ph": 6.3},
    "Cotton": {"rainfall_mm": 680, "temperature_c": 29, "soil_ph": 6.8},
    "Sugarcane": {"rainfall_mm": 1240, "temperature_c": 30, "soil_ph": 6.6},
}

RAIN_ADJUSTMENTS = {"low": -140, "normal": 0, "high": 140}
FERTILIZER_ADJUSTMENTS = {"low": -18, "medium": 0, "high": 18}
PEST_LEVEL_MAP = {"low": 2.5, "medium": 5.0, "high": 7.5}
SEASON_PROFILES = {
    "Kharif": {
        "rainfall_factor": 1.18,
        "temperature_offset": 1.1,
        "humidity_factor": 1.12,
        "pest_offset": 0.8,
        "nutrient_factor": 1.03,
        "summary": "Monsoon crop cycle with stronger rainfall, humidity, and pest pressure.",
    },
    "Rabi": {
        "rainfall_factor": 0.72,
        "temperature_offset": -2.4,
        "humidity_factor": 0.82,
        "pest_offset": -0.4,
        "nutrient_factor": 0.98,
        "summary": "Cooler winter crop cycle with lower rainfall and more stable humidity.",
    },
    "Zaid": {
        "rainfall_factor": 0.48,
        "temperature_offset": 2.8,
        "humidity_factor": 0.74,
        "pest_offset": 0.2,
        "nutrient_factor": 1.04,
        "summary": "Short summer crop cycle with hotter days, lighter rainfall, and faster moisture loss.",
    },
}

COUNTRY_LOCATION_PROFILES = {
    "India": {
        "description": "Monsoon-driven crop planning with strong north-south seasonal variation.",
        "adjustments": {
            "rainfall_mm": 80,
            "temperature_c": 0.8,
            "humidity_pct": 4,
            "soil_ph": 0.0,
            "nitrogen_kg_ha": 4,
            "phosphorus_kg_ha": 2,
            "potassium_kg_ha": 3,
        },
        "directions": {
            "North": {"region": "North", "summary": "Cooler plains with strong wheat and rice belts."},
            "South": {"region": "South", "summary": "Warmer, humid belts with rice and sugarcane strength."},
            "East": {"region": "East", "summary": "Higher humidity with strong rainfall support for cereals."},
            "West": {"region": "West", "summary": "Hotter and relatively drier production zones."},
        },
    },
    "United States": {
        "description": "Large-scale mechanized farming with diverse climate bands across regions.",
        "adjustments": {
            "rainfall_mm": -40,
            "temperature_c": -0.5,
            "humidity_pct": -3,
            "soil_ph": 0.1,
            "nitrogen_kg_ha": 7,
            "phosphorus_kg_ha": 3,
            "potassium_kg_ha": 4,
        },
        "directions": {
            "North": {"region": "North", "summary": "Cooler seasons with moderate rainfall and stable grain belts."},
            "South": {"region": "South", "summary": "Longer warm seasons suited for cotton, maize, and cane."},
            "East": {"region": "East", "summary": "Humid production regions with dependable seasonal moisture."},
            "West": {"region": "West", "summary": "Drier zones where irrigation planning matters more."},
        },
    },
    "Brazil": {
        "description": "Tropical and subtropical crop conditions with strong rainfall influence.",
        "adjustments": {
            "rainfall_mm": 130,
            "temperature_c": 1.1,
            "humidity_pct": 6,
            "soil_ph": -0.1,
            "nitrogen_kg_ha": 5,
            "phosphorus_kg_ha": 2,
            "potassium_kg_ha": 5,
        },
        "directions": {
            "North": {"region": "North", "summary": "Very humid tropical profile with strong rainfall pressure."},
            "South": {"region": "South", "summary": "More balanced temperatures with broad crop suitability."},
            "East": {"region": "East", "summary": "Coastal humidity supports high vegetation growth."},
            "West": {"region": "West", "summary": "Expanding interior agriculture with mixed rainfall conditions."},
        },
    },
    "Australia": {
        "description": "Dryland management focus with stronger dependence on rainfall timing.",
        "adjustments": {
            "rainfall_mm": -140,
            "temperature_c": 1.6,
            "humidity_pct": -8,
            "soil_ph": 0.2,
            "nitrogen_kg_ha": -3,
            "phosphorus_kg_ha": 0,
            "potassium_kg_ha": 1,
        },
        "directions": {
            "North": {"region": "North", "summary": "Hotter growing conditions with stronger weather stress."},
            "South": {"region": "South", "summary": "Relatively cooler conditions with better cereal stability."},
            "East": {"region": "East", "summary": "More dependable coastal moisture than inland zones."},
            "West": {"region": "West", "summary": "Dryer profile where irrigation and nutrient balance matter."},
        },
    },
    "Egypt": {
        "description": "Irrigation-dependent agriculture with hot and arid field conditions.",
        "adjustments": {
            "rainfall_mm": -220,
            "temperature_c": 2.0,
            "humidity_pct": -10,
            "soil_ph": 0.3,
            "nitrogen_kg_ha": 2,
            "phosphorus_kg_ha": 1,
            "potassium_kg_ha": 2,
        },
        "directions": {
            "North": {"region": "North", "summary": "Slightly milder coastal influence with moderate humidity."},
            "South": {"region": "South", "summary": "Hotter interior profile with strong irrigation dependency."},
            "East": {"region": "East", "summary": "Arid production zones where water stress is more likely."},
            "West": {"region": "West", "summary": "Desert-facing pressure with high temperature exposure."},
        },
    },
}


@dataclass
class ModelResult:
    name: str
    pipeline: Pipeline
    rmse: float
    mae: float
    r2: float
    rank: int = 0
    selection_reason: str = ""


class SQLiteCursorWrapper:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._cursor.close()
        return False

    def execute(self, query, params=()):
        self._cursor.execute(query, params)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount


class SQLiteConnectionWrapper:
    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection

    def cursor(self):
        return SQLiteCursorWrapper(self._connection.cursor())

    def execute(self, *args, **kwargs):
        return self._connection.execute(*args, **kwargs)

    def commit(self):
        return self._connection.commit()

    def close(self):
        return self._connection.close()


def get_db_connection():
    global ACTIVE_DB_BACKEND

    if ACTIVE_DB_BACKEND == "postgres":
        try:
            return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        except psycopg2.OperationalError:
            if DB_MODE == "postgres":
                raise
            ACTIVE_DB_BACKEND = "sqlite"

    LOCAL_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(LOCAL_DB_PATH)
    connection.row_factory = sqlite3.Row
    return SQLiteConnectionWrapper(connection)


def run_query(cursor, query: str, params: tuple = ()):
    if ACTIVE_DB_BACKEND == "sqlite":
        query = query.replace("%s", "?")
    cursor.execute(query, params)


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


def ensure_sqlite_column(connection, table_name: str, column_name: str, definition: str) -> None:
    existing_columns = {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name not in existing_columns:
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def init_auth_db() -> None:
    connection = get_db_connection()
    try:
        if ACTIVE_DB_BACKEND == "postgres":
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        full_name TEXT NOT NULL,
                        email TEXT NOT NULL UNIQUE,
                        password_hash TEXT NOT NULL,
                        last_login_at TIMESTAMP,
                        last_active_at TIMESTAMP
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS predictions (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        crop TEXT,
                        yield_value DOUBLE PRECISION,
                        risk TEXT,
                        crop_type TEXT,
                        region TEXT,
                        predicted_yield DOUBLE PRECISION,
                        risk_level TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_predictions_user
                            FOREIGN KEY(user_id)
                            REFERENCES users(id)
                            ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS crop TEXT")
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS yield_value DOUBLE PRECISION")
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS risk TEXT")
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS crop_type TEXT")
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS region TEXT")
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS predicted_yield DOUBLE PRECISION")
                cursor.execute("ALTER TABLE predictions ADD COLUMN IF NOT EXISTS risk_level TEXT")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS advisory_preferences (
                        user_id INTEGER PRIMARY KEY,
                        email_alerts_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        in_app_alerts_enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        favorite_crop TEXT,
                        preferred_region TEXT,
                        preferred_season TEXT,
                        alert_frequency TEXT NOT NULL DEFAULT 'weekly',
                        last_digest_sent_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_advisory_preferences_user
                            FOREIGN KEY(user_id)
                            REFERENCES users(id)
                            ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_notifications (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        advisory_type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        message TEXT NOT NULL,
                        priority TEXT NOT NULL DEFAULT 'normal',
                        cta_label TEXT,
                        cta_href TEXT,
                        metadata_json TEXT,
                        in_app_visible BOOLEAN NOT NULL DEFAULT TRUE,
                        is_read BOOLEAN NOT NULL DEFAULT FALSE,
                        email_delivery_status TEXT NOT NULL DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        sent_at TIMESTAMP,
                        read_at TIMESTAMP,
                        CONSTRAINT fk_user_notifications_user
                            FOREIGN KEY(user_id)
                            REFERENCES users(id)
                            ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS alerts_log (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        alert_signature TEXT NOT NULL,
                        alert_type TEXT NOT NULL,
                        channel TEXT NOT NULL,
                        delivery_status TEXT NOT NULL DEFAULT 'logged',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_alerts_log_user
                            FOREIGN KEY(user_id)
                            REFERENCES users(id)
                            ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_advisory_state (
                        user_id INTEGER PRIMARY KEY,
                        last_best_crop TEXT,
                        last_best_yield DOUBLE PRECISION,
                        last_watch_crop TEXT,
                        last_watch_yield DOUBLE PRECISION,
                        last_watch_risk DOUBLE PRECISION,
                        last_watch_rank INTEGER,
                        last_summary_sent_at TIMESTAMP,
                        last_engine_run_at TIMESTAMP,
                        last_change_type TEXT,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_user_advisory_state_user
                            FOREIGN KEY(user_id)
                            REFERENCES users(id)
                            ON DELETE CASCADE
                    )
                    """
                )
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS email_alerts_enabled BOOLEAN NOT NULL DEFAULT TRUE")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS in_app_alerts_enabled BOOLEAN NOT NULL DEFAULT TRUE")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS favorite_crop TEXT")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS preferred_region TEXT")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS preferred_season TEXT")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS alert_frequency TEXT NOT NULL DEFAULT 'weekly'")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS last_digest_sent_at TIMESTAMP")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                cursor.execute("ALTER TABLE advisory_preferences ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP")
                cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active_at TIMESTAMP")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS advisory_type TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS title TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS message TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal'")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS cta_label TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS cta_href TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS metadata_json TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS in_app_visible BOOLEAN NOT NULL DEFAULT TRUE")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS is_read BOOLEAN NOT NULL DEFAULT FALSE")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS email_delivery_status TEXT NOT NULL DEFAULT 'pending'")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS sent_at TIMESTAMP")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS read_at TIMESTAMP")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'general'")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS confidence_score DOUBLE PRECISION")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS confidence_label TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS expected_impact_pct DOUBLE PRECISION")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS recommendation TEXT")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS popup_dismissed BOOLEAN NOT NULL DEFAULT FALSE")
                cursor.execute("ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS action_json TEXT")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_best_crop TEXT")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_best_yield DOUBLE PRECISION")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_watch_crop TEXT")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_watch_yield DOUBLE PRECISION")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_watch_risk DOUBLE PRECISION")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_watch_rank INTEGER")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_summary_sent_at TIMESTAMP")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_engine_run_at TIMESTAMP")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS last_change_type TEXT")
                cursor.execute("ALTER TABLE user_advisory_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS alerts_log (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        alert_signature TEXT NOT NULL,
                        alert_type TEXT NOT NULL,
                        channel TEXT NOT NULL,
                        delivery_status TEXT NOT NULL DEFAULT 'logged',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_alerts_log_user
                            FOREIGN KEY(user_id)
                            REFERENCES users(id)
                            ON DELETE CASCADE
                    )
                    """
                )
        else:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    full_name TEXT NOT NULL,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    last_login_at TEXT,
                    last_active_at TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS predictions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    crop TEXT,
                    yield_value REAL,
                    risk TEXT,
                    crop_type TEXT,
                    region TEXT,
                    predicted_yield REAL,
                    risk_level TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
            ensure_sqlite_column(connection, "predictions", "crop", "TEXT")
            ensure_sqlite_column(connection, "predictions", "yield_value", "REAL")
            ensure_sqlite_column(connection, "predictions", "risk", "TEXT")
            ensure_sqlite_column(connection, "predictions", "crop_type", "TEXT")
            ensure_sqlite_column(connection, "predictions", "region", "TEXT")
            ensure_sqlite_column(connection, "predictions", "predicted_yield", "REAL")
            ensure_sqlite_column(connection, "predictions", "risk_level", "TEXT")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS advisory_preferences (
                    user_id INTEGER PRIMARY KEY,
                    email_alerts_enabled INTEGER NOT NULL DEFAULT 1,
                    in_app_alerts_enabled INTEGER NOT NULL DEFAULT 1,
                    favorite_crop TEXT,
                    preferred_region TEXT,
                    preferred_season TEXT,
                    alert_frequency TEXT NOT NULL DEFAULT 'weekly',
                    last_digest_sent_at TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS user_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    advisory_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    priority TEXT NOT NULL DEFAULT 'normal',
                    cta_label TEXT,
                    cta_href TEXT,
                    metadata_json TEXT,
                    in_app_visible INTEGER NOT NULL DEFAULT 1,
                    is_read INTEGER NOT NULL DEFAULT 0,
                    email_delivery_status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sent_at TEXT,
                    read_at TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    alert_signature TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    delivery_status TEXT NOT NULL DEFAULT 'logged',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS user_advisory_state (
                    user_id INTEGER PRIMARY KEY,
                    last_best_crop TEXT,
                    last_best_yield REAL,
                    last_watch_crop TEXT,
                    last_watch_yield REAL,
                    last_watch_risk REAL,
                    last_watch_rank INTEGER,
                    last_summary_sent_at TEXT,
                    last_engine_run_at TEXT,
                    last_change_type TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                )
                """
            )
            ensure_sqlite_column(connection, "advisory_preferences", "email_alerts_enabled", "INTEGER NOT NULL DEFAULT 1")
            ensure_sqlite_column(connection, "advisory_preferences", "in_app_alerts_enabled", "INTEGER NOT NULL DEFAULT 1")
            ensure_sqlite_column(connection, "advisory_preferences", "favorite_crop", "TEXT")
            ensure_sqlite_column(connection, "advisory_preferences", "preferred_region", "TEXT")
            ensure_sqlite_column(connection, "advisory_preferences", "preferred_season", "TEXT")
            ensure_sqlite_column(connection, "advisory_preferences", "alert_frequency", "TEXT NOT NULL DEFAULT 'weekly'")
            ensure_sqlite_column(connection, "advisory_preferences", "last_digest_sent_at", "TEXT")
            ensure_sqlite_column(connection, "advisory_preferences", "created_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ensure_sqlite_column(connection, "advisory_preferences", "updated_at", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            ensure_sqlite_column(connection, "users", "last_login_at", "TEXT")
            ensure_sqlite_column(connection, "users", "last_active_at", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "advisory_type", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "title", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "message", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "priority", "TEXT NOT NULL DEFAULT 'normal'")
            ensure_sqlite_column(connection, "user_notifications", "cta_label", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "cta_href", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "metadata_json", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "in_app_visible", "INTEGER NOT NULL DEFAULT 1")
            ensure_sqlite_column(connection, "user_notifications", "is_read", "INTEGER NOT NULL DEFAULT 0")
            ensure_sqlite_column(connection, "user_notifications", "email_delivery_status", "TEXT NOT NULL DEFAULT 'pending'")
            ensure_sqlite_column(connection, "user_notifications", "sent_at", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "read_at", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "category", "TEXT DEFAULT 'general'")
            ensure_sqlite_column(connection, "user_notifications", "confidence_score", "REAL")
            ensure_sqlite_column(connection, "user_notifications", "confidence_label", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "expected_impact_pct", "REAL")
            ensure_sqlite_column(connection, "user_notifications", "recommendation", "TEXT")
            ensure_sqlite_column(connection, "user_notifications", "popup_dismissed", "INTEGER NOT NULL DEFAULT 0")
            ensure_sqlite_column(connection, "user_notifications", "action_json", "TEXT")
            ensure_sqlite_column(connection, "user_advisory_state", "last_best_crop", "TEXT")
            ensure_sqlite_column(connection, "user_advisory_state", "last_best_yield", "REAL")
            ensure_sqlite_column(connection, "user_advisory_state", "last_watch_crop", "TEXT")
            ensure_sqlite_column(connection, "user_advisory_state", "last_watch_yield", "REAL")
            ensure_sqlite_column(connection, "user_advisory_state", "last_watch_risk", "REAL")
            ensure_sqlite_column(connection, "user_advisory_state", "last_watch_rank", "INTEGER")
            ensure_sqlite_column(connection, "user_advisory_state", "last_summary_sent_at", "TEXT")
            ensure_sqlite_column(connection, "user_advisory_state", "last_engine_run_at", "TEXT")
            ensure_sqlite_column(connection, "user_advisory_state", "last_change_type", "TEXT")
        connection.commit()
    finally:
        connection.close()


def get_current_user() -> dict[str, str] | None:
    user_id = session.get("user_id")
    if not user_id:
        return None

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                "SELECT id, full_name, email FROM users WHERE id = %s",
                (user_id,),
            )
            row = cursor.fetchone()
    finally:
        connection.close()

    user = row_to_dict(row)
    if not user:
        session.clear()
        return None
    return user


def touch_user_activity(user_id: int, *, login: bool = False) -> None:
    init_auth_db()
    for attempt in range(2):
        connection = get_db_connection()
        now_iso = datetime.utcnow().isoformat()
        try:
            with connection.cursor() as cursor:
                if login:
                    run_query(
                        cursor,
                        """
                        UPDATE users
                        SET last_login_at = %s,
                            last_active_at = %s
                        WHERE id = %s
                        """,
                        (now_iso, now_iso, user_id),
                    )
                else:
                    run_query(
                        cursor,
                        """
                        UPDATE users
                        SET last_active_at = %s
                        WHERE id = %s
                        """,
                        (now_iso, user_id),
                    )
            connection.commit()
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if ACTIVE_DB_BACKEND == "sqlite" and attempt == 0 and (
                "no such column: last_active_at" in message or "no such column: last_login_at" in message
            ):
                ensure_sqlite_column(connection, "users", "last_login_at", "TEXT")
                ensure_sqlite_column(connection, "users", "last_active_at", "TEXT")
                connection.commit()
                continue
            raise
        finally:
            connection.close()


def safe_float(value: float) -> float:
    return float(round(value, 3))


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.replace(tzinfo=None)
        return parsed
    except ValueError:
        return None


def boolify(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def db_bool(value) -> bool | int:
    normalized = bool(value)
    if ACTIVE_DB_BACKEND == "postgres":
        return normalized
    return 1 if normalized else 0


def advisory_frequency_interval(frequency: str | None) -> timedelta:
    normalized = (frequency or "weekly").strip().lower()
    if normalized == "instant":
        return timedelta(hours=24)
    if normalized == "daily":
        return timedelta(hours=24)
    return timedelta(days=7)


def advisory_scan_interval() -> timedelta:
    return timedelta(hours=max(24, int(os.getenv("ADVISORY_SCAN_HOURS", "24"))))


def severity_rank(level: str | None) -> int:
    mapping = {"info": 0, "important": 1, "critical": 2}
    return mapping.get((level or "info").strip().lower(), 0)


def severity_label(level: str | None) -> str:
    normalized = (level or "info").strip().lower()
    if normalized == "critical":
        return "Action Needed"
    if normalized == "important":
        return "Important"
    return "Info"


def ui_tone(category: str | None, priority: str | None = None) -> str:
    normalized_category = (category or "").strip().lower()
    normalized_priority = (priority or "").strip().lower()
    if normalized_category == "risk" or normalized_priority == "critical":
        return "risk"
    if normalized_category in {"watch", "summary"}:
        return "watch"
    return "opportunity"


def category_icon(category: str | None, title: str | None = None) -> str:
    normalized = (category or "").strip().lower()
    title_text = (title or "").lower()
    if "pest" in title_text:
        return "🐛"
    if "rain" in title_text:
        return "🌧"
    if normalized == "risk":
        return "⚠️"
    if normalized == "watch":
        return "🌧"
    if normalized == "summary":
        return "📊"
    if normalized == "opportunity":
        return "🌾"
    return "🚀"


def describe_confidence(score: float) -> str:
    if score >= 90:
        return "High reliability"
    if score >= 78:
        return "Moderate certainty"
    return "Watch with caution"


def risk_level_rank(level: str | None) -> int:
    mapping = {"low": 0, "moderate": 1, "high": 2}
    return mapping.get(str(level or "").strip().lower(), 0)


def get_activity_cutoff(days: int = 7) -> datetime:
    return datetime.utcnow() - timedelta(days=days)


def get_existing_alert_signatures(user_id: int) -> set[str]:
    connection = get_db_connection()
    signatures: set[str] = set()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT alert_signature
                FROM alerts_log
                WHERE user_id = %s
                  AND created_at >= %s
                """,
                (user_id, (datetime.utcnow() - timedelta(days=14)).isoformat()),
            )
            for row in cursor.fetchall():
                payload = row_to_dict(row) or {}
                signature = payload.get("alert_signature")
                if signature:
                    signatures.add(str(signature))
    finally:
        connection.close()
    return signatures


def log_alert_events(user_id: int, advisories: list[dict[str, object]], *, channel: str, status: str) -> None:
    if not advisories:
        return
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            for advisory in advisories:
                signature = str(advisory.get("signature") or "").strip()
                if not signature:
                    continue
                run_query(
                    cursor,
                    """
                    INSERT INTO alerts_log (
                        user_id,
                        alert_signature,
                        alert_type,
                        channel,
                        delivery_status
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        signature,
                        advisory.get("advisory_type", "system"),
                        channel,
                        status,
                    ),
                )
        connection.commit()
    finally:
        connection.close()


def was_alert_emailed_recently(user_id: int, alert_signature: str, hours: int = 24) -> bool:
    if not alert_signature:
        return False
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT id
                FROM alerts_log
                WHERE user_id = %s
                  AND alert_signature = %s
                  AND channel IN (%s, %s)
                  AND created_at >= %s
                LIMIT 1
                """,
                (
                    user_id,
                    alert_signature,
                    "email",
                    "email_manual",
                    (datetime.utcnow() - timedelta(hours=hours)).isoformat(),
                ),
            )
            row = cursor.fetchone()
    finally:
        connection.close()
    return bool(row)


def advisory_ui_priority(item: dict[str, object]) -> int:
    tone = ui_tone(item.get("category"), item.get("priority"))
    priority_map = {
        "risk": 3,
        "opportunity": 2,
        "watch": 1,
    }
    return priority_map.get(tone, 0)


def dedupe_and_limit_advisories(advisories: list[dict[str, object]]) -> list[dict[str, object]]:
    if not advisories:
        return []

    deduped: list[dict[str, object]] = []
    seen_keys: set[tuple[str, str, str]] = set()
    for item in advisories:
        tone = ui_tone(item.get("category"), item.get("priority"))
        crop = str(item.get("watch_crop") or item.get("best_crop") or "").strip().lower()
        region = str(item.get("region") or "").strip().lower()
        dedupe_key = (crop, region, tone)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(item)

    deduped = sorted(
        deduped,
        key=lambda item: (
            -advisory_ui_priority(item),
            -float(item.get("expected_impact_pct") or 0),
            str(item.get("advisory_type") or ""),
        ),
    )

    selected: list[dict[str, object]] = []
    tones_used: set[str] = set()
    for tone_name in ("risk", "opportunity", "watch"):
        for item in deduped:
            tone = ui_tone(item.get("category"), item.get("priority"))
            if tone == tone_name and tone not in tones_used:
                selected.append(item)
                tones_used.add(tone)
                break

    for item in deduped:
        if len(selected) >= MAX_VISIBLE_ALERTS:
            break
        if item in selected:
            continue
        selected.append(item)

    return selected[:MAX_VISIBLE_ALERTS]


def is_meaningful_alert(item: dict[str, object]) -> bool:
    return str(item.get("advisory_type") or "").strip() in MEANINGFUL_ALERT_TYPES


def build_actionable_recommendations(metrics: dict[str, object]) -> list[str]:
    actions: list[str] = []
    if float(metrics.get("pest_risk", 0) or 0) > 7:
        actions.append("Apply preventive pest control within 3 to 5 days.")
    elif float(metrics.get("pest_risk", 0) or 0) > 5.5:
        actions.append("Increase field scouting because pest pressure is rising.")
    if float(metrics.get("rainfall_mm", 0) or 0) < 500:
        actions.append("Plan irrigation support because moisture availability is likely below ideal.")
    elif float(metrics.get("rainfall_mm", 0) or 0) > 1100:
        actions.append("Strengthen drainage checks because rainfall is above the comfort band.")
    if float(metrics.get("soil_ph", 7) or 7) < 6:
        actions.append("Consider lime treatment to improve pH balance and nutrient uptake.")
    elif float(metrics.get("soil_ph", 7) or 7) > 7.8:
        actions.append("Review sulfur or organic correction because soil alkalinity is elevated.")
    if float(metrics.get("humidity_pct", 0) or 0) > 78:
        actions.append("Keep disease monitoring tighter because humidity is high.")
    if not actions:
        actions.append("Maintain the current field plan and monitor the next advisory cycle.")
    return actions


def compact_action_text(notification: dict[str, object]) -> str:
    recommendation = str(notification.get("recommendation") or "").strip()
    if recommendation:
        return recommendation
    tone = ui_tone(notification.get("category"), notification.get("priority"))
    if tone == "risk":
        return "Take action soon"
    if tone == "watch":
        return "Keep monitoring"
    return "No action needed"


def compact_title_text(notification: dict[str, object]) -> str:
    title = str(notification.get("title") or "").strip()
    if not title:
        return "Field update"
    replacements = {
        "Top crop changed in ": "",
        "Weekly advisory summary for ": "Weekly summary: ",
        " risk increased in ": " risk high in ",
        " yield outlook improved": " yield improved",
        " yield outlook softened": " yield softer",
        " moved up the regional ranking": " climbed ranking",
        " remains stable in ": " stable in ",
    }
    compact = title
    for source, target in replacements.items():
        compact = compact.replace(source, target)
    return compact


def serialize_notification_for_ui(notification: dict[str, object]) -> dict[str, object]:
    item = dict(notification)
    item["priority"] = (item.get("priority") or "info").strip().lower()
    item["category"] = (item.get("category") or "general").strip().lower()
    item["ui_tone"] = ui_tone(item["category"], item["priority"])
    item["priority_label"] = {
        "risk": "Action Needed",
        "watch": "Watch",
        "opportunity": "Opportunity",
    }.get(item["ui_tone"], severity_label(item["priority"]))
    item["icon"] = category_icon(item["category"], item.get("title"))
    item["compact_title"] = compact_title_text(item)
    item["compact_action"] = compact_action_text(item)
    item["region_label"] = "Local advisory"
    message = str(item.get("message") or "")
    if " in " in message:
        try:
            item["region_label"] = message.split(" in ", 1)[1].split(".", 1)[0].strip().title()
        except Exception:
            item["region_label"] = "Local advisory"
    score = float(item.get("confidence_score") or 0)
    filled = max(1, min(10, round(score / 10))) if score else 0
    item["confidence_bar"] = f"{'█' * filled}{'░' * max(0, 10 - filled)} {int(round(score))}%"
    metadata_raw = item.get("metadata_json")
    if metadata_raw:
        try:
            metadata = json.loads(metadata_raw)
        except json.JSONDecodeError:
            metadata = {}
    else:
        metadata = {}
    item["region_name"] = metadata.get("region") or item["region_label"]
    item["season_name"] = metadata.get("season")
    item["signature"] = metadata.get("signature")
    item["best_crop"] = metadata.get("best_crop")
    item["best_yield"] = metadata.get("best_yield")
    item["best_risk_level"] = metadata.get("best_risk_level")
    item["watch_crop"] = metadata.get("watch_crop")
    item["watch_yield"] = metadata.get("watch_yield")
    item["watch_risk"] = metadata.get("watch_risk")
    item["watch_risk_level"] = metadata.get("watch_risk_level")
    return item


def serialize_preferences(row: dict[str, object] | None) -> dict[str, object] | None:
    if not row:
        return None
    payload = dict(row)
    payload["email_alerts_enabled"] = bool(payload.get("email_alerts_enabled"))
    payload["in_app_alerts_enabled"] = bool(payload.get("in_app_alerts_enabled"))
    payload["favorite_crop"] = payload.get("favorite_crop") or ""
    payload["preferred_region"] = payload.get("preferred_region") or ""
    payload["preferred_season"] = payload.get("preferred_season") or "Kharif"
    payload["alert_frequency"] = payload.get("alert_frequency") or "weekly"
    return payload


def get_recent_prediction_profile(user_id: int) -> dict[str, object] | None:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT
                    COALESCE(crop_type, crop) AS crop_type,
                    region,
                    created_at
                FROM predictions
                WHERE user_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (user_id,),
            )
            return row_to_dict(cursor.fetchone())
    finally:
        connection.close()


def build_default_advisory_preferences(user_id: int, state: dict[str, object] | None = None) -> dict[str, object]:
    state = state or load_project_state()
    dataset: pd.DataFrame = state["dataset"]
    recent_profile = get_recent_prediction_profile(user_id)
    favorite_crop = (
        recent_profile.get("crop_type")
        if recent_profile and recent_profile.get("crop_type")
        else str(dataset["crop_type"].mode().iat[0])
    )
    preferred_region = (
        recent_profile.get("region")
        if recent_profile and recent_profile.get("region")
        else str(dataset["region"].mode().iat[0])
    )
    return {
        "user_id": user_id,
        "email_alerts_enabled": True,
        "in_app_alerts_enabled": True,
        "favorite_crop": favorite_crop,
        "preferred_region": preferred_region,
        "preferred_season": "Kharif",
        "alert_frequency": "weekly",
        "last_digest_sent_at": None,
    }


def get_advisory_state(user_id: int) -> dict[str, object] | None:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT
                    user_id,
                    last_best_crop,
                    last_best_yield,
                    last_watch_crop,
                    last_watch_yield,
                    last_watch_risk,
                    last_watch_rank,
                    last_summary_sent_at,
                    last_engine_run_at,
                    last_change_type,
                    updated_at
                FROM user_advisory_state
                WHERE user_id = %s
                """,
                (user_id,),
            )
            return row_to_dict(cursor.fetchone())
    finally:
        connection.close()


def save_advisory_state(user_id: int, snapshot: dict[str, object], change_type: str = "stable") -> None:
    existing = get_advisory_state(user_id)
    now_iso = datetime.utcnow().isoformat()
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            if existing:
                run_query(
                    cursor,
                    """
                    UPDATE user_advisory_state
                    SET
                        last_best_crop = %s,
                        last_best_yield = %s,
                        last_watch_crop = %s,
                        last_watch_yield = %s,
                        last_watch_risk = %s,
                        last_watch_rank = %s,
                        last_engine_run_at = %s,
                        last_change_type = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                    """,
                    (
                        snapshot["best_crop"],
                        snapshot["best_yield"],
                        snapshot["watch_crop"],
                        snapshot["watch_yield"],
                        snapshot["watch_risk"],
                        snapshot["watch_rank"],
                        now_iso,
                        change_type,
                        user_id,
                    ),
                )
            else:
                run_query(
                    cursor,
                    """
                    INSERT INTO user_advisory_state (
                        user_id,
                        last_best_crop,
                        last_best_yield,
                        last_watch_crop,
                        last_watch_yield,
                        last_watch_risk,
                        last_watch_rank,
                        last_engine_run_at,
                        last_change_type,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    """,
                    (
                        user_id,
                        snapshot["best_crop"],
                        snapshot["best_yield"],
                        snapshot["watch_crop"],
                        snapshot["watch_yield"],
                        snapshot["watch_risk"],
                        snapshot["watch_rank"],
                        now_iso,
                        change_type,
                    ),
                )
        connection.commit()
    finally:
        connection.close()


def get_or_create_advisory_preferences(
    user_id: int,
    state: dict[str, object] | None = None,
    overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT
                    user_id,
                    email_alerts_enabled,
                    in_app_alerts_enabled,
                    favorite_crop,
                    preferred_region,
                    preferred_season,
                    alert_frequency,
                    last_digest_sent_at,
                    created_at,
                    updated_at
                FROM advisory_preferences
                WHERE user_id = %s
                """,
                (user_id,),
            )
            existing = row_to_dict(cursor.fetchone())
            if existing:
                return serialize_preferences(existing)

            defaults = build_default_advisory_preferences(user_id, state)
            if overrides:
                defaults.update(overrides)
            run_query(
                cursor,
                """
                INSERT INTO advisory_preferences (
                    user_id,
                    email_alerts_enabled,
                    in_app_alerts_enabled,
                    favorite_crop,
                    preferred_region,
                    preferred_season,
                    alert_frequency,
                    last_digest_sent_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """,
                (
                    user_id,
                    db_bool(defaults["email_alerts_enabled"]),
                    db_bool(defaults["in_app_alerts_enabled"]),
                    defaults["favorite_crop"],
                    defaults["preferred_region"],
                    defaults["preferred_season"],
                    defaults["alert_frequency"],
                    defaults["last_digest_sent_at"],
                ),
            )
        connection.commit()
    finally:
        connection.close()

    return serialize_preferences(defaults)


def update_advisory_preferences(user_id: int, payload: dict[str, object], state: dict[str, object] | None = None) -> dict[str, object]:
    current = get_or_create_advisory_preferences(user_id, state=state)
    updated = {
        "email_alerts_enabled": boolify(payload.get("email_alerts_enabled", current["email_alerts_enabled"])),
        "in_app_alerts_enabled": boolify(payload.get("in_app_alerts_enabled", current["in_app_alerts_enabled"])),
        "favorite_crop": str(payload.get("favorite_crop") or current["favorite_crop"] or "").strip(),
        "preferred_region": str(payload.get("preferred_region") or current["preferred_region"] or "").strip(),
        "preferred_season": str(payload.get("preferred_season") or current["preferred_season"] or "Kharif").strip(),
        "alert_frequency": str(payload.get("alert_frequency") or current["alert_frequency"] or "weekly").strip().lower(),
    }
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                UPDATE advisory_preferences
                SET
                    email_alerts_enabled = %s,
                    in_app_alerts_enabled = %s,
                    favorite_crop = %s,
                    preferred_region = %s,
                    preferred_season = %s,
                    alert_frequency = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                """,
                (
                    db_bool(updated["email_alerts_enabled"]),
                    db_bool(updated["in_app_alerts_enabled"]),
                    updated["favorite_crop"],
                    updated["preferred_region"],
                    updated["preferred_season"],
                    updated["alert_frequency"],
                    user_id,
                ),
            )
        connection.commit()
    finally:
        connection.close()
    current.update(updated)
    return current


def build_advisory_candidate(
    crop: str,
    crop_frame: pd.DataFrame,
    region: str,
    season: str,
) -> dict[str, float | str]:
    averaged = crop_frame.mean(numeric_only=True)
    mode_soil = (
        crop_frame["soil_type"].mode().iat[0]
        if not crop_frame["soil_type"].mode().empty
        else str(crop_frame["soil_type"].iloc[0])
    )
    candidate: dict[str, float | str] = {
        "crop_type": crop,
        "region": region or str(crop_frame["region"].mode().iat[0]),
        "season": season,
        "soil_type": mode_soil,
        "rainfall_mm": safe_float(float(averaged.get("rainfall_mm", 0.0))),
        "temperature_c": safe_float(float(averaged.get("temperature_c", 0.0))),
        "humidity_pct": safe_float(float(averaged.get("humidity_pct", 0.0))),
        "soil_ph": safe_float(float(averaged.get("soil_ph", 7.0))),
        "nitrogen_kg_ha": safe_float(float(averaged.get("nitrogen_kg_ha", 0.0))),
        "phosphorus_kg_ha": safe_float(float(averaged.get("phosphorus_kg_ha", 0.0))),
        "potassium_kg_ha": safe_float(float(averaged.get("potassium_kg_ha", 0.0))),
        "pest_risk": safe_float(float(averaged.get("pest_risk", 0.0))),
    }
    season_profile = SEASON_PROFILES.get(season)
    if season_profile:
        candidate["rainfall_mm"] = safe_float(float(candidate["rainfall_mm"]) * float(season_profile["rainfall_factor"]))
        candidate["temperature_c"] = safe_float(float(candidate["temperature_c"]) + float(season_profile["temperature_offset"]))
        candidate["humidity_pct"] = safe_float(min(95.0, max(25.0, float(candidate["humidity_pct"]) * float(season_profile["humidity_factor"]))))
        candidate["nitrogen_kg_ha"] = safe_float(float(candidate["nitrogen_kg_ha"]) * float(season_profile["nutrient_factor"]))
        candidate["phosphorus_kg_ha"] = safe_float(float(candidate["phosphorus_kg_ha"]) * float(season_profile["nutrient_factor"]))
        candidate["potassium_kg_ha"] = safe_float(float(candidate["potassium_kg_ha"]) * float(season_profile["nutrient_factor"]))
        candidate["pest_risk"] = safe_float(
            min(10.0, max(0.0, float(candidate["pest_risk"]) + float(season_profile["pest_offset"])))
        )
    return candidate


def build_advisory_snapshot(preferences: dict[str, object], state: dict[str, object]) -> dict[str, object]:
    dataset: pd.DataFrame = state["dataset"]
    best_model: ModelResult = state["best_model"]
    region = preferences.get("preferred_region") or ""
    season = preferences.get("preferred_season") or "Kharif"
    favorite_crop = preferences.get("favorite_crop") or ""

    scoped = dataset.copy()
    if region and region in dataset["region"].unique():
        scoped = scoped[scoped["region"] == region]
    if scoped.empty:
        scoped = dataset.copy()

    annual_grouped = (
        scoped.groupby("crop_type", as_index=False)
        .agg(
            avg_yield=("yield_ton_per_hectare", "mean"),
        )
        .sort_values("avg_yield", ascending=False)
        .reset_index(drop=True)
    )
    seasonal_rankings: list[dict[str, object]] = []
    for crop_name in sorted(scoped["crop_type"].unique().tolist()):
        crop_frame = scoped[scoped["crop_type"] == crop_name]
        if crop_frame.empty:
            continue
        candidate = build_advisory_candidate(crop_name, crop_frame, region or str(crop_frame["region"].mode().iat[0]), season)
        input_frame = pd.DataFrame([candidate], columns=FEATURE_COLUMNS)
        predicted_yield = safe_float(best_model.pipeline.predict(input_frame)[0])
        risk = calculate_risk(candidate)
        seasonal_rankings.append(
            {
                "crop_type": crop_name,
                "predicted_yield": predicted_yield,
                "risk_score": risk["score"],
                "risk_level": risk["level"],
                "candidate": candidate,
            }
        )
    seasonal_rankings = sorted(
        seasonal_rankings,
        key=lambda item: (-float(item["predicted_yield"]), float(item["risk_score"]), str(item["crop_type"])),
    )
    for index, item in enumerate(seasonal_rankings, start=1):
        item["rank"] = index

    top_row = seasonal_rankings[0]
    favorite_row = next((item for item in seasonal_rankings if item["crop_type"] == favorite_crop), top_row)
    favorite_crop = favorite_crop or str(favorite_row["crop_type"])
    confidence_score = safe_float(float(state["best_model"].r2) * 100)
    favorite_candidate = favorite_row["candidate"]
    return {
        "region": region or "All regions",
        "season": season,
        "annual_best_crop": str(annual_grouped.iloc[0]["crop_type"]),
        "annual_best_yield": safe_float(float(annual_grouped.iloc[0]["avg_yield"])),
        "best_crop": str(top_row["crop_type"]),
        "best_yield": safe_float(float(top_row["predicted_yield"])),
        "best_risk_level": str(top_row["risk_level"]),
        "best_risk_score": safe_float(float(top_row["risk_score"])),
        "watch_crop": favorite_crop,
        "watch_yield": safe_float(float(favorite_row["predicted_yield"])),
        "watch_risk": safe_float(float(favorite_row["risk_score"])),
        "watch_risk_level": str(favorite_row["risk_level"]),
        "watch_rank": int(favorite_row["rank"]),
        "watch_metrics": {
            "rainfall_mm": safe_float(float(favorite_candidate["rainfall_mm"])),
            "humidity_pct": safe_float(float(favorite_candidate["humidity_pct"])),
            "pest_risk": safe_float(float(favorite_candidate["pest_risk"])),
            "soil_ph": safe_float(float(favorite_candidate["soil_ph"])),
        },
        "top_action": f"Grow {top_row['crop_type']}",
        "season_favors_change": str(top_row["crop_type"]) != str(annual_grouped.iloc[0]["crop_type"]),
        "model_name": state["best_model"].name,
        "model_confidence_score": confidence_score,
        "model_confidence_label": describe_confidence(confidence_score),
        "recommendations": build_actionable_recommendations(favorite_candidate),
    }


def build_primary_recommendation(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "title": f"Grow {snapshot['best_crop']}",
        "crop": snapshot["best_crop"],
        "region": snapshot["region"],
        "yield": snapshot["best_yield"],
        "risk_level": snapshot.get("best_risk_level", "Low"),
        "confidence_score": snapshot["model_confidence_score"],
        "confidence_bar": f"{'█' * max(1, min(10, round(float(snapshot['model_confidence_score']) / 10)))}{'░' * max(0, 10 - max(1, min(10, round(float(snapshot['model_confidence_score']) / 10))))} {int(round(float(snapshot['model_confidence_score'])))}%",
        "action": snapshot.get("top_action") or f"Grow {snapshot['best_crop']}",
        "reason": snapshot["recommendations"][0] if snapshot.get("recommendations") else "Conditions currently support this crop best.",
    }


def build_advisory_event(
    event_type: str,
    category: str,
    severity: str,
    title: str,
    message: str,
    snapshot: dict[str, object],
    *,
    cta_label: str,
    cta_href: str,
    expected_impact_pct: float | None = None,
) -> dict[str, object]:
    default_recommendation = snapshot["recommendations"][0] if snapshot.get("recommendations") else "Review the dashboard recommendation panel."
    event_recommendations = {
        "best_crop_changed": f"Consider switching to {snapshot['best_crop']}.",
        "yield_improvement": "Consider switching crop.",
        "risk_increase": "Apply pest control in 3 days.",
        "season_shift": f"Plan around {snapshot['season']} conditions.",
        "watchlist_stable": "No action needed.",
        "watchlist_started": "No action needed.",
        "reengagement": "Check updated crop predictions.",
    }
    recommendation = event_recommendations.get(event_type, default_recommendation)
    return {
        "advisory_type": event_type,
        "category": category,
        "priority": severity,
        "title": title,
        "message": message,
        "cta_label": cta_label,
        "cta_href": cta_href,
        "confidence_score": snapshot["model_confidence_score"],
        "confidence_label": snapshot["model_confidence_label"],
        "expected_impact_pct": expected_impact_pct,
        "recommendation": recommendation,
        "action_items": snapshot.get("recommendations", []),
        "region": snapshot["region"],
        "season": snapshot["season"],
        "best_crop": snapshot["best_crop"],
        "best_yield": snapshot["best_yield"],
        "best_risk_level": snapshot.get("best_risk_level"),
        "watch_crop": snapshot["watch_crop"],
        "watch_yield": snapshot["watch_yield"],
        "watch_risk": snapshot["watch_risk"],
        "watch_risk_level": snapshot.get("watch_risk_level"),
        "signature": f"{event_type}::{snapshot['region']}::{snapshot['season']}::{snapshot['best_crop']}::{snapshot['watch_crop']}::{expected_impact_pct}",
    }


def generate_triggered_advisories(
    user: dict[str, object],
    preferences: dict[str, object],
    state: dict[str, object],
    previous_state: dict[str, object] | None,
) -> tuple[list[dict[str, object]], dict[str, object], str]:
    snapshot = build_advisory_snapshot(preferences, state)
    advisories: list[dict[str, object]] = []
    change_type = "stable"
    previous_best_crop = previous_state.get("last_best_crop") if previous_state else None
    previous_best_yield = float(previous_state.get("last_best_yield") or 0) if previous_state else 0.0
    previous_watch_yield = float(previous_state.get("last_watch_yield") or 0) if previous_state else 0.0
    previous_watch_risk = float(previous_state.get("last_watch_risk") or 0) if previous_state else 0.0
    previous_watch_rank = int(previous_state.get("last_watch_rank") or snapshot["watch_rank"]) if previous_state else snapshot["watch_rank"]

    if previous_best_crop and previous_best_crop != snapshot["best_crop"]:
        change_type = "best_crop_changed"
        advisories.append(
            build_advisory_event(
                "best_crop_changed",
                "opportunity",
                "important",
                "New Best Crop Detected",
                f"{snapshot['best_crop']} is now best in {snapshot['region']}.",
                snapshot,
                cta_label="View details",
                cta_href="#advisory-center",
                expected_impact_pct=safe_float(((snapshot["best_yield"] - previous_best_yield) / previous_best_yield) * 100) if previous_best_yield else None,
            )
        )

    if previous_watch_yield > 0:
        yield_gain_pct = safe_float(((snapshot["watch_yield"] - previous_watch_yield) / previous_watch_yield) * 100)
        if yield_gain_pct >= 15:
            if change_type == "stable":
                change_type = "yield_improvement"
            advisories.append(
                build_advisory_event(
                    "yield_improvement",
                    "opportunity",
                    "important",
                    "Yield Opportunity",
                    f"{snapshot['watch_crop']} yield increased by {yield_gain_pct}% in {snapshot['region']}.",
                    snapshot,
                    cta_label="Compare crops",
                    cta_href="#workflow-review",
                    expected_impact_pct=yield_gain_pct,
                )
            )

    previous_risk_level = "high" if previous_watch_risk >= 67 else "moderate" if previous_watch_risk >= 34 else "low"
    current_risk_level = str(snapshot["watch_risk_level"]).lower()
    if risk_level_rank(previous_risk_level) < risk_level_rank("high") and current_risk_level == "high":
        if change_type == "stable":
            change_type = "risk_increase"
        advisories.append(
            build_advisory_event(
                "risk_increase",
                "risk",
                "critical",
                "Risk Increased",
                f"{snapshot['watch_crop']} risk is now HIGH in {snapshot['region']}.",
                snapshot,
                cta_label="View why",
                cta_href="#advisory-center",
                expected_impact_pct=safe_float(max(snapshot["watch_risk"] - previous_watch_risk, 0)),
            )
        )

    if snapshot["season_favors_change"]:
        if change_type == "stable":
            change_type = "season_shift"
        advisories.append(
            build_advisory_event(
                "season_shift",
                "watch",
                "important",
                "Season Update",
                f"{snapshot['season']} now favors {snapshot['best_crop']} over {snapshot['annual_best_crop']}.",
                snapshot,
                cta_label="View details",
                cta_href="#advisory-center",
            )
        )
    return dedupe_and_limit_advisories(advisories), snapshot, change_type


def build_weekly_summary_advisory(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "advisory_type": "weekly_summary",
        "category": "summary",
        "priority": "important",
        "title": f"Weekly advisory summary for {snapshot['region']}",
        "message": f"Best crop: {snapshot['best_crop']} at about {snapshot['best_yield']} ton/hectare. Watch crop: {snapshot['watch_crop']} ranks #{snapshot['watch_rank']} with risk score {snapshot['watch_risk']}. Reliability: {snapshot['model_confidence_label']} ({snapshot['model_confidence_score']}%).",
        "cta_label": "Open advisory center",
        "cta_href": "#advisory-center",
        "confidence_score": snapshot["model_confidence_score"],
        "confidence_label": snapshot["model_confidence_label"],
        "expected_impact_pct": None,
        "recommendation": snapshot["recommendations"][0],
        "action_items": snapshot["recommendations"],
        "region": snapshot["region"],
        "season": snapshot["season"],
        "best_crop": snapshot["best_crop"],
        "best_yield": snapshot["best_yield"],
        "best_risk_level": snapshot.get("best_risk_level"),
        "watch_crop": snapshot["watch_crop"],
        "watch_yield": snapshot["watch_yield"],
        "watch_risk": snapshot["watch_risk"],
        "watch_risk_level": snapshot.get("watch_risk_level"),
        "signature": f"weekly-summary::{snapshot['region']}::{snapshot['season']}::{snapshot['best_crop']}::{datetime.utcnow().date().isoformat()}",
    }


def insert_notifications(user_id: int, advisories: list[dict[str, object]], preferences: dict[str, object]) -> list[dict[str, object]]:
    existing_signatures = get_existing_alert_signatures(user_id)
    fresh_items = [item for item in advisories if item.get("signature") not in existing_signatures]
    if not fresh_items:
        return []

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            for advisory in fresh_items:
                metadata_json = json.dumps(
                    {
                        "signature": advisory["signature"],
                        "region": advisory.get("region"),
                        "season": advisory.get("season"),
                        "best_crop": advisory.get("best_crop"),
                        "best_yield": advisory.get("best_yield"),
                        "best_risk_level": advisory.get("best_risk_level"),
                        "watch_crop": advisory.get("watch_crop"),
                        "watch_yield": advisory.get("watch_yield"),
                        "watch_risk": advisory.get("watch_risk"),
                        "watch_risk_level": advisory.get("watch_risk_level"),
                    }
                )
                action_json = json.dumps(advisory.get("action_items", []))
                run_query(
                    cursor,
                    """
                    INSERT INTO user_notifications (
                        user_id,
                        advisory_type,
                        category,
                        title,
                        message,
                        priority,
                        cta_label,
                        cta_href,
                        metadata_json,
                        action_json,
                        recommendation,
                        confidence_score,
                        confidence_label,
                        expected_impact_pct,
                        popup_dismissed,
                        in_app_visible,
                        is_read,
                        email_delivery_status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        advisory["advisory_type"],
                        advisory.get("category", "general"),
                        advisory["title"],
                        advisory["message"],
                        advisory.get("priority", "info"),
                        advisory.get("cta_label"),
                        advisory.get("cta_href"),
                        metadata_json,
                        action_json,
                        advisory.get("recommendation"),
                        advisory.get("confidence_score"),
                        advisory.get("confidence_label"),
                        advisory.get("expected_impact_pct"),
                        db_bool(severity_rank(advisory.get("priority")) < 1),
                        db_bool(preferences.get("in_app_alerts_enabled")),
                        db_bool(False),
                        "pending" if preferences.get("email_alerts_enabled") and severity_rank(advisory.get("priority")) >= 1 else "disabled",
                    ),
                )
        connection.commit()
    finally:
        connection.close()
    log_alert_events(user_id, fresh_items, channel="in_app", status="queued")
    return fresh_items


def can_send_email_alerts() -> bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_FROM_EMAIL"))


def get_smtp_status() -> dict[str, object]:
    required = {
        "SMTP_HOST": os.getenv("SMTP_HOST"),
        "SMTP_PORT": os.getenv("SMTP_PORT"),
        "SMTP_FROM_EMAIL": os.getenv("SMTP_FROM_EMAIL"),
    }
    optional = {
        "SMTP_USERNAME": os.getenv("SMTP_USERNAME"),
        "SMTP_PASSWORD": os.getenv("SMTP_PASSWORD"),
    }
    missing_required = [key for key, value in required.items() if not value]
    return {
        "ready": len(missing_required) == 0,
        "missing_required": missing_required,
        "has_auth": bool(optional["SMTP_USERNAME"] and optional["SMTP_PASSWORD"]),
    }


def send_email(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    smtp_status = get_smtp_status()
    if not smtp_status["ready"]:
        missing = ", ".join(smtp_status["missing_required"]) or "SMTP settings"
        return False, f"smtp-not-configured: {missing}"

    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")
    from_email = os.getenv("SMTP_FROM_EMAIL")
    use_tls = boolify(os.getenv("SMTP_USE_TLS", "1"))

    message = MIMEText(body, "plain", "utf-8")
    message["Subject"] = subject
    message["From"] = from_email
    message["To"] = to_email

    try:
        with smtplib.SMTP(host, port, timeout=20) as smtp:
            if use_tls:
                smtp.starttls()
            if username:
                smtp.login(username, password or "")
            smtp.sendmail(from_email, [to_email], message.as_string())
    except Exception as exc:  # pragma: no cover
        return False, str(exc)
    return True, "sent"


def send_advisory_email(user: dict[str, object], advisories: list[dict[str, object]]) -> tuple[bool, str]:
    if not advisories:
        return False, "no-content"
    app_url = os.getenv("APP_BASE_URL", "").rstrip("/")
    top_alert = sorted(
        advisories,
        key=lambda item: (-severity_rank(item.get("priority")), item.get("advisory_type") != "best_crop_changed"),
    )[0]

    detail_link = f"{app_url}{top_alert.get('cta_href')}" if app_url and str(top_alert.get("cta_href", "")).startswith("#") else (app_url or top_alert.get("cta_href") or "")
    body_lines = [
        top_alert["title"],
        top_alert["message"],
        f"📈 Yield: {top_alert.get('best_yield', top_alert.get('watch_yield', 'N/A'))} t/ha",
        f"⚠️ Risk: {str(top_alert.get('best_risk_level') or top_alert.get('watch_risk_level') or 'Watch').title()}",
        f"👉 View Details {detail_link}".strip(),
    ]
    return send_email(user["email"], top_alert["title"], "\n".join(body_lines))


def update_notification_email_status(user_id: int, sent_at: str | None, status: str) -> None:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                UPDATE user_notifications
                SET email_delivery_status = %s,
                    sent_at = %s
                WHERE user_id = %s
                  AND email_delivery_status = 'pending'
                """,
                (status, sent_at, user_id),
            )
            if status == "sent":
                run_query(
                    cursor,
                    """
                    UPDATE advisory_preferences
                    SET last_digest_sent_at = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = %s
                    """,
                    (sent_at, user_id),
                )
        connection.commit()
    finally:
        connection.close()


def should_send_weekly_summary(preferences: dict[str, object], previous_state: dict[str, object] | None, force: bool = False) -> bool:
    if preferences.get("alert_frequency") != "weekly" and not force:
        return False
    if not previous_state or not previous_state.get("last_summary_sent_at"):
        return True
    last_summary = parse_timestamp(previous_state.get("last_summary_sent_at"))
    if not last_summary:
        return True
    return datetime.utcnow() - last_summary >= timedelta(days=7)


def update_weekly_summary_state(user_id: int) -> None:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                UPDATE user_advisory_state
                SET last_summary_sent_at = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = %s
                """,
                (datetime.utcnow().isoformat(), user_id),
            )
        connection.commit()
    finally:
        connection.close()


def maybe_generate_advisories(
    user: dict[str, object],
    state: dict[str, object],
    force: bool = False,
) -> dict[str, object]:
    preferences = get_or_create_advisory_preferences(user["id"], state=state)
    previous_state = get_advisory_state(user["id"])
    last_engine_run = parse_timestamp(previous_state.get("last_engine_run_at")) if previous_state else None
    if not force and last_engine_run and datetime.utcnow() - last_engine_run < advisory_scan_interval():
        return preferences

    generated, snapshot, change_type = generate_triggered_advisories(user, preferences, state, previous_state)
    if should_send_weekly_summary(preferences, previous_state, force=force):
        generated.append(build_weekly_summary_advisory(snapshot))
    fresh_items = insert_notifications(user["id"], generated, preferences)
    email_candidates = [
        item for item in fresh_items
        if severity_rank(item.get("priority")) >= 1
        and not was_alert_emailed_recently(user["id"], str(item.get("signature") or ""), hours=24)
    ]
    if email_candidates and preferences.get("email_alerts_enabled"):
        email_ok, email_status = send_advisory_email(user, email_candidates)
        sent_at = datetime.utcnow().isoformat() if email_ok else None
        update_notification_email_status(user["id"], sent_at, "sent" if email_ok else email_status)
        log_alert_events(user["id"], email_candidates[:1], channel="email", status="sent" if email_ok else email_status)
    elif fresh_items:
        update_notification_email_status(user["id"], None, "disabled")
    save_advisory_state(user["id"], snapshot, change_type=change_type)
    if any(item.get("advisory_type") == "weekly_summary" for item in fresh_items):
        update_weekly_summary_state(user["id"])
    return preferences


def list_user_notifications(
    user_id: int,
    include_read: bool = True,
    limit: int = 12,
    category: str | None = None,
    severity: str | None = None,
) -> list[dict[str, object]]:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    id,
                    advisory_type,
                    category,
                    title,
                    message,
                    priority,
                    cta_label,
                    cta_href,
                    recommendation,
                    confidence_score,
                    confidence_label,
                    expected_impact_pct,
                    metadata_json,
                    action_json,
                    popup_dismissed,
                    in_app_visible,
                    is_read,
                    email_delivery_status,
                    created_at,
                    read_at
                FROM user_notifications
                WHERE user_id = %s
                  AND in_app_visible = %s
            """
            params: list[object] = [user_id, db_bool(True)]
            if not include_read:
                query += " AND is_read = %s"
                params.append(db_bool(False))
            if category and category != "all":
                query += " AND category = %s"
                params.append(category)
            if severity and severity != "all":
                query += " AND priority = %s"
                params.append(severity)
            query += " ORDER BY created_at DESC, id DESC LIMIT %s"
            params.append(limit)
            run_query(cursor, query, tuple(params))
            rows = cursor.fetchall()
    finally:
        connection.close()

    notifications: list[dict[str, object]] = []
    for row in rows:
        item = row_to_dict(row)
        item["is_read"] = bool(item.get("is_read"))
        item["in_app_visible"] = bool(item.get("in_app_visible"))
        item["popup_dismissed"] = bool(item.get("popup_dismissed"))
        try:
            item["action_items"] = json.loads(item.get("action_json") or "[]")
        except json.JSONDecodeError:
            item["action_items"] = []
        if is_meaningful_alert(item):
            notifications.append(serialize_notification_for_ui(item))
    return dedupe_and_limit_advisories(notifications)


def get_unread_notification_count(user_id: int) -> int:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT COUNT(*) AS unread_count
                FROM user_notifications
                WHERE user_id = %s
                  AND in_app_visible = %s
                  AND is_read = %s
                """,
                (user_id, db_bool(True), db_bool(False)),
            )
            row = row_to_dict(cursor.fetchone()) or {}
    finally:
        connection.close()
    return int(row.get("unread_count") or 0)


def get_priority_popup_notification(user_id: int) -> dict[str, object] | None:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT
                    id,
                    advisory_type,
                    category,
                    title,
                    message,
                    priority,
                    cta_label,
                    cta_href,
                    recommendation,
                    confidence_score,
                    confidence_label,
                    expected_impact_pct,
                    metadata_json,
                    action_json,
                    popup_dismissed,
                    created_at
                FROM user_notifications
                WHERE user_id = %s
                  AND in_app_visible = %s
                  AND is_read = %s
                  AND popup_dismissed = %s
                  AND (priority = %s OR priority = %s)
                ORDER BY CASE WHEN priority = 'critical' THEN 0 ELSE 1 END, created_at DESC, id DESC
                LIMIT 1
                """,
                (user_id, db_bool(True), db_bool(False), db_bool(False), "critical", "important"),
            )
            row = row_to_dict(cursor.fetchone())
    finally:
        connection.close()
    if not row:
        return None
    row["popup_dismissed"] = bool(row.get("popup_dismissed"))
    try:
        row["action_items"] = json.loads(row.get("action_json") or "[]")
    except json.JSONDecodeError:
        row["action_items"] = []
    return serialize_notification_for_ui(row)


def mark_notification_as_read(user_id: int, notification_id: int, dismiss_popup: bool = False) -> bool:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                UPDATE user_notifications
                SET is_read = %s,
                    read_at = %s,
                    popup_dismissed = %s
                WHERE id = %s
                  AND user_id = %s
                """,
                (db_bool(True), datetime.utcnow().isoformat(), db_bool(dismiss_popup), notification_id, user_id),
            )
            updated = cursor.rowcount
        connection.commit()
    finally:
        connection.close()
    return bool(updated)


def dismiss_notification_popup(user_id: int, notification_id: int) -> bool:
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                UPDATE user_notifications
                SET popup_dismissed = %s
                WHERE id = %s
                  AND user_id = %s
                """,
                (db_bool(True), notification_id, user_id),
            )
            updated = cursor.rowcount
        connection.commit()
    finally:
        connection.close()
    return bool(updated)


def run_advisory_engine_for_all_users(force: bool = False) -> int:
    init_auth_db()
    state = load_project_state()
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(cursor, "SELECT id, full_name, email, last_login_at, last_active_at FROM users", ())
            users = [row_to_dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()

    processed = 0
    for user in users:
        if not user:
            continue
        maybe_generate_advisories(user, state, force=force)
        processed += 1
    return processed


def build_preprocessor() -> ColumnTransformer:
    return ColumnTransformer(
        transformers=[
            ("categorical", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_COLUMNS),
            ("numerical", StandardScaler(), NUMERICAL_COLUMNS),
        ]
    )


def build_ga_optimized_ann(
    x_train: pd.DataFrame, y_train: pd.Series
) -> Pipeline:
    settings = MODEL_EVAL_SETTINGS
    pipeline = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor()),
            (
                "model",
                MLPRegressor(
                    max_iter=settings["ga_ann_max_iter"],
                    random_state=42,
                    early_stopping=True,
                ),
            ),
        ]
    )
    search = RandomizedSearchCV(
        estimator=pipeline,
        param_distributions={
            "model__hidden_layer_sizes": [(32,), (64,), (96,), (64, 32), (48, 24)],
            "model__learning_rate_init": [0.001, 0.003, 0.01],
            "model__alpha": [0.0001, 0.001, 0.01],
        },
        n_iter=settings["ga_search_iterations"],
        cv=settings["ga_search_cv"],
        random_state=42,
        scoring="r2",
        n_jobs=1,
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        search.fit(x_train, y_train)
    return search.best_estimator_


def evaluate_models(dataframe: pd.DataFrame) -> dict[str, object]:
    settings = MODEL_EVAL_SETTINGS
    x = dataframe[FEATURE_COLUMNS]
    y = dataframe["yield_ton_per_hectare"]

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42
    )

    baseline_ann_pipeline = Pipeline(
        steps=[
            ("preprocessor", build_preprocessor()),
            (
                "model",
                MLPRegressor(
                    hidden_layer_sizes=(64, 32),
                    max_iter=settings["baseline_ann_max_iter"],
                    random_state=42,
                    early_stopping=True,
                ),
            ),
        ]
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        baseline_ann_pipeline.fit(x_train, y_train)
        baseline_predictions = baseline_ann_pipeline.predict(x_test)
    baseline_ann_metrics = {
        "rmse": safe_float(np.sqrt(mean_squared_error(y_test, baseline_predictions))),
        "mae": safe_float(mean_absolute_error(y_test, baseline_predictions)),
        "r2": safe_float(r2_score(y_test, baseline_predictions)),
    }

    ga_ann_pipeline = build_ga_optimized_ann(x_train, y_train)
    candidates = [
        (
            "Linear Regression",
            Pipeline(
                steps=[
                    ("preprocessor", build_preprocessor()),
                    ("model", LinearRegression()),
                ]
            ),
        ),
        (
            "Random Forest",
            Pipeline(
                steps=[
                    ("preprocessor", build_preprocessor()),
                    (
                        "model",
                        RandomForestRegressor(
                            n_estimators=settings["rf_estimators"],
                            random_state=42,
                            max_depth=settings["rf_max_depth"],
                            n_jobs=1,
                        ),
                    ),
                ]
            ),
        ),
        (
            "Gradient Boosting",
            Pipeline(
                steps=[
                    ("preprocessor", build_preprocessor()),
                    (
                        "model",
                        GradientBoostingRegressor(
                            n_estimators=settings["gb_estimators"],
                            learning_rate=settings["gb_learning_rate"],
                            max_depth=3,
                            random_state=42,
                        ),
                    ),
                ]
            ),
        ),
        ("GA-Optimized ANN", ga_ann_pipeline),
    ]

    results: list[ModelResult] = []
    for name, pipeline in candidates:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ConvergenceWarning)
            if name != "GA-Optimized ANN":
                pipeline.fit(x_train, y_train)
            predictions = pipeline.predict(x_test)
        results.append(
            ModelResult(
                name=name,
                pipeline=pipeline,
                rmse=safe_float(np.sqrt(mean_squared_error(y_test, predictions))),
                mae=safe_float(mean_absolute_error(y_test, predictions)),
                r2=safe_float(r2_score(y_test, predictions)),
            )
        )

    results = sorted(results, key=lambda result: (result.r2, -result.rmse, -result.mae), reverse=True)
    best_model = results[0]
    for index, item in enumerate(results, start=1):
        item.rank = index
    for item in results:
        item.selection_reason = build_model_selection_reason(item, best_model)
    ga_ann_result = next((item for item in results if item.name == "GA-Optimized ANN"), None)
    ga_ann_improvement = None
    if ga_ann_result:
        baseline_rmse = baseline_ann_metrics["rmse"]
        if baseline_rmse > 0:
            rmse_gain_pct = safe_float(((baseline_rmse - ga_ann_result.rmse) / baseline_rmse) * 100)
        else:
            rmse_gain_pct = 0.0
        ga_ann_improvement = {
            "baseline_rmse": baseline_rmse,
            "optimized_rmse": ga_ann_result.rmse,
            "baseline_r2": baseline_ann_metrics["r2"],
            "optimized_r2": ga_ann_result.r2,
            "rmse_gain_pct": rmse_gain_pct,
        }
    return {
        "results": results,
        "best_model": best_model,
        "x_test": x_test,
        "y_test": y_test,
        "ga_ann_improvement": ga_ann_improvement,
    }


def get_active_dataset_path() -> Path:
    return CUSTOM_DATASET_PATH if CUSTOM_DATASET_PATH.exists() else DATASET_PATH


def validate_dataset(dataframe: pd.DataFrame) -> tuple[bool, str | None]:
    expected_columns = FEATURE_COLUMNS + ["yield_ton_per_hectare"]
    missing_columns = [column for column in expected_columns if column not in dataframe.columns]
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"

    if len(dataframe) < 30:
        return False, "Dataset must contain at least 30 rows for training."

    try:
        numeric_columns = [
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
        dataframe[numeric_columns] = dataframe[numeric_columns].apply(pd.to_numeric)
    except Exception:
        return False, "Numeric columns contain invalid values."

    supported_crops = set(IDEAL_GROWTH.keys())
    found_crops = set(dataframe["crop_type"].astype(str).unique())
    unsupported_crops = sorted(found_crops - supported_crops)
    if unsupported_crops:
        return False, (
            "Unsupported crop_type values found: "
            + ", ".join(unsupported_crops)
            + ". Supported crops are: "
            + ", ".join(sorted(supported_crops))
        )

    return True, None


def calculate_risk(inputs: dict[str, float | str]) -> dict[str, object]:
    profile = IDEAL_GROWTH[str(inputs["crop_type"])]

    rainfall_gap = abs(float(inputs["rainfall_mm"]) - profile["rainfall_mm"])
    temperature_gap = abs(float(inputs["temperature_c"]) - profile["temperature_c"])
    soil_gap = abs(float(inputs["soil_ph"]) - profile["soil_ph"])

    rainfall_deviation = min((rainfall_gap / 600) ** 0.92, 1.0)
    temperature_stress = min((temperature_gap / 15) ** 0.9, 1.0)
    soil_condition = min((soil_gap / 2.5) ** 0.88, 1.0)
    pest_risk = min((float(inputs["pest_risk"]) / 10) ** 0.96, 1.0)

    factor_scores = {
        "rainfall_deviation": rainfall_deviation,
        "temperature_stress": temperature_stress,
        "soil_condition": soil_condition,
        "pest_risk": pest_risk,
    }

    fuzzy_memberships = {
        "rainfall_deviation": min(max((rainfall_gap - 80) / 520, 0), 1),
        "temperature_stress": min(max((temperature_gap - 2) / 10, 0), 1),
        "soil_condition": min(max((soil_gap - 0.15) / 1.6, 0), 1),
        "pest_risk": min(max((float(inputs["pest_risk"]) - 2) / 7, 0), 1),
    }
    fuzzy_score = sum(fuzzy_memberships[key] * RISK_WEIGHTS[key] for key in fuzzy_memberships)
    weighted_score = sum(factor_scores[key] * RISK_WEIGHTS[key] for key in factor_scores)
    combined_score = (weighted_score * 0.45) + (fuzzy_score * 0.55)
    score_out_of_100 = round(combined_score * 100, 1)

    if score_out_of_100 < 35:
        level = "Low"
    elif score_out_of_100 < 65:
        level = "Moderate"
    else:
        level = "High"

    dominant_factors = sorted(factor_scores.items(), key=lambda item: item[1], reverse=True)
    key_labels = {
        "rainfall_deviation": "rainfall deviation",
        "temperature_stress": "temperature deviation",
        "soil_condition": "soil condition",
        "pest_risk": "pest pressure",
    }
    active_labels = [key_labels[key] for key, value in dominant_factors if value >= 0.18][:2]
    explanation = (
        "Conditions are relatively stable, so no single stress factor is dominating the risk."
        if not active_labels
        else f"Risk is mainly influenced by {' and '.join(active_labels)}."
    )

    return {
        "score": score_out_of_100,
        "level": level,
        "factors": {key: safe_float(value) for key, value in factor_scores.items()},
        "fuzzy_factors": {key: safe_float(value) for key, value in fuzzy_memberships.items()},
        "explanation": explanation,
        "engine": "Fuzzy Logic Risk Engine",
    }


def build_recommendations(inputs: dict[str, float | str], risk: dict[str, object]) -> list[str]:
    recommendations: list[str] = []
    factors = risk["factors"]

    if factors["rainfall_deviation"] > 0.45:
        recommendations.append("Adjust irrigation planning because rainfall is far from the crop's ideal range.")
    if factors["temperature_stress"] > 0.45:
        recommendations.append("Prepare for temperature stress with mulch, shade planning, or heat-tolerant crop management.")
    if factors["soil_condition"] > 0.40:
        recommendations.append("Improve soil balance by monitoring pH and reviewing nutrient amendments before the next cycle.")
    if factors["pest_risk"] > 0.35:
        recommendations.append("Increase pest surveillance and preventive treatment because pest risk is elevated.")
    if not recommendations:
        recommendations.append("Current conditions are relatively stable. Continue routine monitoring and balanced nutrient management.")

    return recommendations


def build_action_cards(inputs: dict[str, float | str], risk: dict[str, object]) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    factors = risk["factors"]

    if factors["rainfall_deviation"] > 0.35:
        cards.append(
            {
                "title": "Irrigation Planning",
                "icon": "Water",
                "tone": "warning",
                "detail": "Rainfall is away from the crop benchmark. Adjust irrigation timing and monitor field moisture regularly.",
            }
        )
    if float(inputs["nitrogen_kg_ha"]) < 80 or float(inputs["phosphorus_kg_ha"]) < 40:
        cards.append(
            {
                "title": "Fertilizer Balance",
                "icon": "NPK",
                "tone": "accent",
                "detail": "Nutrient levels are on the lower side. Consider a balanced NPK schedule before the next growth stage.",
            }
        )
    if factors["pest_risk"] > 0.30:
        cards.append(
            {
                "title": "Pest Control",
                "icon": "Shield",
                "tone": "danger",
                "detail": "Pest pressure is elevated. Increase scouting frequency and prepare preventive treatment if symptoms rise.",
            }
        )
    if factors["soil_condition"] > 0.30:
        cards.append(
            {
                "title": "Soil Care",
                "icon": "Soil",
                "tone": "muted",
                "detail": "Soil pH is drifting from the ideal range. Review amendments and maintain soil health before the next cycle.",
            }
        )

    if not cards:
        cards.append(
            {
                "title": "Stable Conditions",
                "icon": "Leaf",
                "tone": "success",
                "detail": "Current field conditions are close to the crop profile. Continue routine irrigation, nutrient balance, and scouting.",
            }
        )

    return cards


def build_insights(inputs: dict[str, float | str], risk: dict[str, object], dataframe: pd.DataFrame) -> list[str]:
    crop = str(inputs["crop_type"])
    crop_rows = dataframe[dataframe["crop_type"] == crop]
    if crop_rows.empty:
        crop_rows = dataframe

    crop_avg_rainfall = float(crop_rows["rainfall_mm"].mean())
    rainfall_delta_pct = ((float(inputs["rainfall_mm"]) - crop_avg_rainfall) / max(crop_avg_rainfall, 1)) * 100
    dominant_factor = max(risk["factors"], key=risk["factors"].get)
    dominant_factor_label = dominant_factor.replace("_", " ").title()

    insights = [
        f"Your rainfall is {abs(rainfall_delta_pct):.1f}% {'above' if rainfall_delta_pct >= 0 else 'below'} the average for {crop}.",
        f"The strongest yield pressure right now is {dominant_factor_label.lower()}.",
    ]

    season = str(inputs.get("season", "")).strip()
    if season in SEASON_PROFILES:
        if season == "Kharif":
            insights.append("Kharif season usually brings higher rainfall and humidity, which can support growth but also raise pest pressure.")
        elif season == "Rabi":
            insights.append("Rabi season tends to be cooler and more stable, which often improves field control but reduces natural rainfall support.")
        elif season == "Zaid":
            insights.append("Zaid season often means hotter days and faster moisture loss, so irrigation timing becomes more important.")

    if float(inputs["temperature_c"]) > float(crop_rows["temperature_c"].mean()) + 2:
        insights.append("Temperature is running warmer than the crop average, so heat stress management can improve reliability.")
    elif float(inputs["pest_risk"]) >= 6:
        insights.append("Pest pressure is the fastest lever to improve output if addressed early.")
    else:
        insights.append("Your current profile is relatively close to training data conditions, which improves trust in the forecast.")

    return insights


def calculate_confidence(dataframe: pd.DataFrame, inputs: dict[str, float | str], best_model: ModelResult) -> dict[str, object]:
    crop_rows = dataframe[dataframe["crop_type"] == str(inputs["crop_type"])]
    if crop_rows.empty:
        crop_rows = dataframe

    distance_parts: list[float] = []
    for column in NUMERICAL_COLUMNS:
        series = crop_rows[column].astype(float)
        std = float(series.std()) if float(series.std()) > 0 else 1.0
        z_score = abs(float(inputs[column]) - float(series.mean())) / std
        distance_parts.append(z_score)

    avg_distance = float(np.mean(distance_parts)) if distance_parts else 0.0
    base_confidence = max(0.25, min(0.97, float(best_model.r2) * 0.75 + (1 / (1 + avg_distance)) * 0.25))
    score = round(base_confidence * 100, 1)

    if score >= 78:
        label = "High confidence"
    elif score >= 58:
        label = "Moderate confidence"
    else:
        label = "Low confidence"

    return {
        "score": score,
        "label": label,
        "explanation": "Confidence is based on model accuracy and how close the input is to known training patterns.",
    }


def build_season_message(inputs: dict[str, object]) -> str | None:
    season = str(inputs.get("season", "")).strip()
    if season not in SEASON_PROFILES:
        return None
    if season == "Kharif":
        return "Kharif season detected — higher rainfall and humidity are expected in this profile."
    if season == "Rabi":
        return "Rabi season detected — cooler conditions and lower rainfall are influencing the current field profile."
    if season == "Zaid":
        return "Zaid season detected — hotter and drier conditions are being considered in this prediction."
    return None


def build_ai_statement(
    inputs: dict[str, object],
    risk: dict[str, object],
    predicted_yield: float,
) -> str:
    crop = str(inputs.get("crop_type", "the selected crop"))
    season = str(inputs.get("season", "")).strip()
    humidity = float(inputs.get("humidity_pct", 0))
    pest = float(inputs.get("pest_risk", 0))
    nutrient_balance = "balanced soil nutrients" if float(inputs.get("nitrogen_kg_ha", 0)) >= 80 and float(inputs.get("phosphorus_kg_ha", 0)) >= 40 else "moderate soil nutrients"
    rainfall_phrase = "seasonal rainfall pattern" if season else "current rainfall pattern"
    pest_phrase = "moderate pest risk" if pest < 6 else "elevated pest risk"
    stability_phrase = "stable" if risk["level"] == "Low" else ("care-managed" if risk["level"] == "Moderate" else "pressured")
    season_prefix = f"{season} season" if season else "current season"
    humidity_phrase = "higher humidity" if humidity >= 70 else "controlled humidity"
    return (
        f"Based on {nutrient_balance}, the {rainfall_phrase} in {season_prefix}, "
        f"{humidity_phrase}, and {pest_phrase}, the system predicts a {stability_phrase} "
        f"{crop.lower()} yield of about {predicted_yield} ton/hectare with {risk['level'].lower()} overall agricultural risk."
    )


def build_model_selection_reason(result: ModelResult, best_model: ModelResult) -> str:
    if result.name == best_model.name:
        return (
            f"Highest R2 score with the strongest balance of RMSE {best_model.rmse} "
            f"and MAE {best_model.mae} on unseen validation data."
        )

    gaps: list[str] = []
    if result.r2 < best_model.r2:
        gaps.append(f"lower R2 ({result.r2} vs {best_model.r2})")
    if result.rmse > best_model.rmse:
        gaps.append(f"higher RMSE ({result.rmse} vs {best_model.rmse})")
    if result.mae > best_model.mae:
        gaps.append(f"higher MAE ({result.mae} vs {best_model.mae})")

    if not gaps:
        return (
            f"Close competitor, but {best_model.name} still delivered the stronger "
            "overall validation balance for live prediction."
        )

    if len(gaps) == 1:
        metrics_text = gaps[0]
    elif len(gaps) == 2:
        metrics_text = f"{gaps[0]} and {gaps[1]}"
    else:
        metrics_text = f"{', '.join(gaps[:-1])}, and {gaps[-1]}"

    return f"Not selected because it showed {metrics_text} than {best_model.name} during evaluation."


def get_reference_rows(dataframe: pd.DataFrame, inputs: dict[str, object]) -> pd.DataFrame:
    crop = str(inputs.get("crop_type", ""))
    region = str(inputs.get("region", ""))

    crop_region_rows = dataframe[
        (dataframe["crop_type"] == crop) & (dataframe["region"] == region)
    ]
    if not crop_region_rows.empty:
        return crop_region_rows

    crop_rows = dataframe[dataframe["crop_type"] == crop]
    if not crop_rows.empty:
        return crop_rows

    region_rows = dataframe[dataframe["region"] == region]
    if not region_rows.empty:
        return region_rows

    return dataframe


def build_improved_management_scenario(
    dataframe: pd.DataFrame, inputs: dict[str, object]
) -> tuple[dict[str, object], list[str]]:
    reference_rows = get_reference_rows(dataframe, inputs)
    improved = dict(inputs)
    changes: list[str] = []

    numeric_means = {
        column: float(reference_rows[column].astype(float).mean())
        for column in NUMERICAL_COLUMNS
    }

    def move_towards_mean(field: str, strength: float = 0.45) -> float:
        current = float(inputs[field])
        target = numeric_means[field]
        return safe_float(current + (target - current) * strength)

    current_rainfall = float(inputs["rainfall_mm"])
    target_rainfall = numeric_means["rainfall_mm"]
    rainfall_gap = target_rainfall - current_rainfall
    if abs(rainfall_gap) > 60:
        improved["rainfall_mm"] = safe_float(current_rainfall + rainfall_gap * 0.4)
        changes.append("irrigation planning moved rainfall closer to the crop-region average")
    else:
        improved["rainfall_mm"] = safe_float(current_rainfall)

    improved["temperature_c"] = move_towards_mean("temperature_c", 0.25)
    improved["humidity_pct"] = max(25.0, min(95.0, move_towards_mean("humidity_pct", 0.3)))

    soil_target = numeric_means["soil_ph"]
    improved["soil_ph"] = max(5.5, min(7.2, safe_float(float(inputs["soil_ph"]) + (soil_target - float(inputs["soil_ph"])) * 0.5)))
    if abs(improved["soil_ph"] - float(inputs["soil_ph"])) >= 0.1:
        changes.append("soil pH is corrected toward a more stable band")

    for nutrient in ("nitrogen_kg_ha", "phosphorus_kg_ha", "potassium_kg_ha"):
        current_value = float(inputs[nutrient])
        target_value = numeric_means[nutrient]
        if current_value < target_value:
            improved[nutrient] = safe_float(current_value + (target_value - current_value) * 0.65)
            changes.append(f"{nutrient.replace('_kg_ha', '').replace('_', ' ')} support is improved")
        else:
            improved[nutrient] = safe_float(current_value + (target_value - current_value) * 0.25)

    current_pest = float(inputs["pest_risk"])
    target_pest = numeric_means["pest_risk"]
    improved["pest_risk"] = max(0.0, safe_float(min(current_pest - 1.5, current_pest + (target_pest - current_pest) * 0.6)))
    if improved["pest_risk"] < current_pest:
        changes.append("pest pressure is reduced through improved field management")

    deduped_changes = list(dict.fromkeys(changes))
    return improved, deduped_changes


def validate_prediction_inputs(payload: dict[str, object]) -> list[str]:
    issues: list[str] = []
    for field in FEATURE_COLUMNS:
        if field not in payload or payload[field] in ("", None):
            issues.append(f"{field} is required.")

    if issues:
        return issues

    if float(payload["rainfall_mm"]) < 0:
        issues.append("Rainfall cannot be negative.")
    if not (0 <= float(payload["humidity_pct"]) <= 100):
        issues.append("Humidity should be between 0 and 100.")
    if not (0 <= float(payload["pest_risk"]) <= 10):
        issues.append("Pest risk should be between 0 and 10.")
    if not (3 <= float(payload["soil_ph"]) <= 10):
        issues.append("Soil pH should be between 3 and 10.")

    return issues


def build_report_html(report_data: dict[str, object]) -> str:
    action_cards = "".join(
        f"<li><strong>{card['title']}:</strong> {card['detail']}</li>"
        for card in report_data["action_cards"]
    )
    insights = "".join(f"<li>{item}</li>" for item in report_data["insights"])
    recommendations = "".join(f"<li>{item}</li>" for item in report_data["recommendations"])

    return f"""
    <html>
    <head>
      <title>Crop Yield Prediction Report</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 32px; color: #1f2a1e; }}
        h1, h2 {{ color: #436850; }}
        .hero {{ padding: 18px; background: #f6f1e6; border-radius: 14px; margin-bottom: 20px; }}
        .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
        .card {{ border: 1px solid #d5dccd; border-radius: 14px; padding: 16px; margin-bottom: 14px; }}
      </style>
    </head>
    <body>
      <div class="hero">
        <h1>Crop Yield Prediction Report</h1>
        <p>Generated on {datetime.now().strftime('%d %B %Y, %I:%M %p')}</p>
        <p><strong>Crop:</strong> {report_data['crop_type']} | <strong>Region:</strong> {report_data['region']}</p>
      </div>
      <div class="grid">
        <div class="card">
          <h2>Prediction Summary</h2>
          <p><strong>Predicted Yield:</strong> {report_data['predicted_yield']} ton/hectare</p>
          <p><strong>Risk Level:</strong> {report_data['risk_level']}</p>
          <p><strong>Confidence:</strong> {report_data['confidence_label']} ({report_data['confidence_score']}%)</p>
          <p><strong>Recommended Crop:</strong> {report_data['recommended_crop']}</p>
        </div>
        <div class="card">
          <h2>Insights</h2>
          <ul>{insights}</ul>
        </div>
      </div>
      <div class="card">
        <h2>Action Cards</h2>
        <ul>{action_cards}</ul>
      </div>
      <div class="card">
        <h2>Recommendations</h2>
        <ul>{recommendations}</ul>
      </div>
    </body>
    </html>
    """


def get_fit_label(score: float) -> str:
    if score <= 0.18:
        return "Excellent fit"
    if score <= 0.32:
        return "Good fit"
    if score <= 0.5:
        return "Moderate fit"
    return "Weak fit"


def get_fit_priority(score: float) -> int:
    if score <= 0.18:
        return 0
    if score <= 0.32:
        return 1
    if score <= 0.5:
        return 2
    return 3


def score_crop_match(inputs: dict[str, float | str], crop: str) -> float:
    ideal = IDEAL_GROWTH[crop]
    score = 0.0
    score += abs(float(inputs["rainfall_mm"]) - ideal["rainfall_mm"]) / 1000
    score += abs(float(inputs["temperature_c"]) - ideal["temperature_c"]) / 20
    score += abs(float(inputs["soil_ph"]) - ideal["soil_ph"]) / 3
    return round(score, 3)


def rank_crop_matches(inputs: dict[str, float | str]) -> list[dict[str, object]]:
    scores: list[dict[str, object]] = []
    for crop in IDEAL_GROWTH.keys():
        rounded_score = score_crop_match(inputs, crop)

        scores.append(
            {
                "crop": crop,
                "match_score": rounded_score,
                "fit_label": get_fit_label(rounded_score),
            }
        )

    return sorted(scores, key=lambda item: item["match_score"])


def build_simple_profile_candidate(
    state: dict[str, object],
    crop: str,
    simple_context: dict[str, str],
) -> dict[str, float | str] | None:
    simple_profiles = state["simple_profiles"]
    location_profiles = state["location_profiles"]
    country = str(simple_context.get("country", ""))
    direction = str(simple_context.get("direction", ""))
    season = str(simple_context.get("season", "Kharif"))
    rain_situation = str(simple_context.get("rain_situation", "normal"))
    fertilizer_level = str(simple_context.get("fertilizer_level", "medium"))
    pest_situation = str(simple_context.get("pest_situation", "low"))

    country_profile = location_profiles.get(country)
    if not country_profile:
        return None

    direction_profile = country_profile["directions"].get(direction)
    if not direction_profile:
        fallback_direction = next(iter(country_profile["directions"].values()), None)
        if fallback_direction is None:
            return None
        direction_profile = fallback_direction

    region = str(direction_profile["region"])
    crop_profiles = simple_profiles.get(crop)
    if not crop_profiles:
        return None

    profile = crop_profiles.get(region)
    if not profile:
        fallback_region = next(iter(crop_profiles.values()), None)
        if fallback_region is None:
            return None
        profile = fallback_region

    adjusted = dict(profile)
    country_adjustments = country_profile.get("adjustments", {})
    nutrient_shift = FERTILIZER_ADJUSTMENTS.get(fertilizer_level, 0)

    adjusted["country"] = country
    adjusted["direction"] = direction
    adjusted["season"] = season
    adjusted["region"] = region
    adjusted["rainfall_mm"] = round(float(adjusted["rainfall_mm"]) + float(country_adjustments.get("rainfall_mm", 0)) + RAIN_ADJUSTMENTS.get(rain_situation, 0), 3)
    adjusted["temperature_c"] = round(float(adjusted["temperature_c"]) + float(country_adjustments.get("temperature_c", 0)), 3)
    adjusted["humidity_pct"] = round(float(adjusted["humidity_pct"]) + float(country_adjustments.get("humidity_pct", 0)), 3)
    adjusted["soil_ph"] = round(float(adjusted["soil_ph"]) + float(country_adjustments.get("soil_ph", 0)), 3)
    adjusted["nitrogen_kg_ha"] = round(float(adjusted["nitrogen_kg_ha"]) + float(country_adjustments.get("nitrogen_kg_ha", 0)) + nutrient_shift, 3)
    adjusted["phosphorus_kg_ha"] = round(float(adjusted["phosphorus_kg_ha"]) + float(country_adjustments.get("phosphorus_kg_ha", 0)) + nutrient_shift * 0.6, 3)
    adjusted["potassium_kg_ha"] = round(float(adjusted["potassium_kg_ha"]) + float(country_adjustments.get("potassium_kg_ha", 0)) + nutrient_shift * 0.7, 3)
    adjusted["pest_risk"] = PEST_LEVEL_MAP.get(pest_situation, 2.5)

    season_profile = SEASON_PROFILES.get(season)
    if season_profile:
        adjusted["rainfall_mm"] = round(float(adjusted["rainfall_mm"]) * float(season_profile["rainfall_factor"]), 3)
        adjusted["temperature_c"] = round(float(adjusted["temperature_c"]) + float(season_profile["temperature_offset"]), 3)
        adjusted["humidity_pct"] = round(float(adjusted["humidity_pct"]) * float(season_profile["humidity_factor"]), 3)
        adjusted["nitrogen_kg_ha"] = round(float(adjusted["nitrogen_kg_ha"]) * float(season_profile["nutrient_factor"]), 3)
        adjusted["phosphorus_kg_ha"] = round(float(adjusted["phosphorus_kg_ha"]) * float(season_profile["nutrient_factor"]), 3)
        adjusted["potassium_kg_ha"] = round(float(adjusted["potassium_kg_ha"]) * float(season_profile["nutrient_factor"]), 3)
        adjusted["pest_risk"] = round(
            min(10.0, max(0.0, float(adjusted["pest_risk"]) + float(season_profile["pest_offset"]))),
            3,
        )
        adjusted["season_summary"] = season_profile["summary"]
        adjusted["season_direction_profile"] = f"{direction} {season}"
    else:
        adjusted["season_summary"] = "Season context not applied."
        adjusted["season_direction_profile"] = direction

    adjusted["humidity_pct"] = round(min(95.0, max(25.0, float(adjusted["humidity_pct"]))), 3)
    return adjusted


def rank_crop_matches_for_simple_context(
    state: dict[str, object],
    simple_context: dict[str, str],
) -> list[dict[str, object]]:
    scores: list[dict[str, object]] = []
    for crop in IDEAL_GROWTH.keys():
        candidate = build_simple_profile_candidate(state, crop, simple_context)
        if not candidate:
            continue
        rounded_score = score_crop_match(candidate, crop)
        scores.append(
            {
                "crop": crop,
                "match_score": rounded_score,
                "fit_label": get_fit_label(rounded_score),
            }
        )
    return sorted(scores, key=lambda item: item["match_score"])


def recommend_crop_for_inputs(inputs: dict[str, float | str], state: dict[str, object] | None = None) -> str:
    simple_context = inputs.get("simple_mode_context")
    if isinstance(simple_context, dict) and state is not None:
        ranked_matches = rank_crop_matches_for_simple_context(state, simple_context)
    else:
        ranked_matches = rank_crop_matches(inputs)
    return ranked_matches[0]["crop"]


def rank_crop_predictions(
    state: dict[str, object],
    inputs: dict[str, object],
) -> list[dict[str, object]]:
    best_model: ModelResult = state["best_model"]
    simple_context = inputs.get("simple_mode_context")
    ranked: list[dict[str, object]] = []

    for crop in IDEAL_GROWTH.keys():
        if isinstance(simple_context, dict):
            candidate = build_simple_profile_candidate(state, crop, simple_context)
        else:
            candidate = dict(inputs)
            candidate["crop_type"] = crop

        if not candidate:
            continue

        input_frame = pd.DataFrame([candidate], columns=FEATURE_COLUMNS)
        predicted_yield = safe_float(best_model.pipeline.predict(input_frame)[0])
        risk = calculate_risk(candidate)
        match_score = score_crop_match(candidate, crop)
        suitability_score = safe_float(predicted_yield - (risk["score"] / 100) * 1.1)
        ranked.append(
            {
                "crop": crop,
                "predicted_yield": predicted_yield,
                "risk_level": risk["level"],
                "risk_score": risk["score"],
                "match_score": match_score,
                "fit_label": get_fit_label(match_score),
                "fit_priority": get_fit_priority(match_score),
                "suitability_score": suitability_score,
            }
        )

    return sorted(
        ranked,
        key=lambda item: (
            item["fit_priority"],
            item["risk_score"],
            -item["predicted_yield"],
            item["match_score"],
        ),
    )


def build_analytics_figures(
    dataframe: pd.DataFrame,
    model_results: list[ModelResult],
    current_input: dict[str, float | str] | None = None,
    predicted_yield: float | None = None,
    risk: dict[str, object] | None = None,
) -> dict[str, object]:
    best_model_name = model_results[0].name if model_results else ""
    model_order = [item.name for item in model_results]
    palette = {
        "Gradient Boosting": "#4f8a3f",
        "Random Forest": "#d28a16",
        "Linear Regression": "#3f7ad6",
        "GA-Optimized ANN": "#118a7e",
    }
    fallback_colors = ["#4f8a3f", "#d28a16", "#3f7ad6", "#118a7e"]
    color_map = {}
    for index, item in enumerate(model_results):
        if item.name == best_model_name:
            color_map[item.name] = "#4f8a3f"
        else:
            color_map[item.name] = palette.get(item.name, fallback_colors[index % len(fallback_colors)])
    model_metrics_df = pd.DataFrame(
        [
            {"Model": item.name, "Metric": "RMSE", "Value": item.rmse}
            for item in model_results
        ]
        + [
            {"Model": item.name, "Metric": "MAE", "Value": item.mae}
            for item in model_results
        ]
        + [
            {"Model": item.name, "Metric": "R2", "Value": item.r2}
            for item in model_results
        ]
    )

    comparison_chart = px.bar(
        model_metrics_df,
        x="Metric",
        y="Value",
        color="Model",
        barmode="group",
        title=f"Model Performance Comparison Across {len(model_results)} Regression Models",
        category_orders={"Model": model_order},
        color_discrete_map=color_map,
    )
    comparison_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=60, b=20),
        legend_title_text="Models",
        hovermode="x unified",
    )
    comparison_chart.update_traces(
        hovertemplate="<b>%{fullData.name}</b><br>%{x}: %{y:.3f}<extra></extra>"
    )
    comparison_chart.add_annotation(
        x=1,
        y=1.12,
        xref="paper",
        yref="paper",
        text=f"Best overall model: {best_model_name}",
        showarrow=False,
        font=dict(size=13, color="#4f8a3f"),
        bgcolor="rgba(244, 251, 241, 0.92)",
        bordercolor="rgba(79, 138, 63, 0.24)",
        borderpad=6,
    )

    yield_by_crop = (
        dataframe.groupby("crop_type", as_index=False)["yield_ton_per_hectare"]
        .mean()
        .sort_values("yield_ton_per_hectare", ascending=False)
    )
    highlighted_crop = str(current_input["crop_type"]) if current_input and current_input.get("crop_type") else None
    yield_by_crop["bar_color"] = yield_by_crop["crop_type"].apply(
        lambda crop: "#d97706" if crop == highlighted_crop else "#436850"
    )
    yield_chart = px.bar(
        yield_by_crop,
        x="crop_type",
        y="yield_ton_per_hectare",
        title=(
            f"Average Yield by Crop with Your {highlighted_crop} Estimate"
            if highlighted_crop
            else "Average Yield by Crop"
        ),
        color="bar_color",
        color_discrete_map="identity",
    )
    if highlighted_crop:
        crop_average = yield_by_crop.loc[
            yield_by_crop["crop_type"] == highlighted_crop, "yield_ton_per_hectare"
        ]
        if not crop_average.empty:
            yield_chart.add_annotation(
                x=highlighted_crop,
                y=float(crop_average.iloc[0]),
                text=f"Current crop: {highlighted_crop}",
                showarrow=True,
                arrowhead=2,
                ay=-45,
                bgcolor="rgba(255,248,234,0.9)",
            )
    if highlighted_crop and predicted_yield is not None:
        yield_chart.add_trace(
            go.Scatter(
                x=[highlighted_crop],
                y=[predicted_yield],
                mode="markers+text",
                marker=dict(size=16, color="#b91c1c", symbol="diamond"),
                text=["Live input yield"],
                textposition="top center",
                name="Live input",
            )
        )
    yield_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=60, b=20),
        showlegend=False,
        xaxis_title="Crop",
        yaxis_title="Average yield (t/ha)",
    )

    risk_bins = pd.cut(
        dataframe["risk_score"],
        bins=[-1, 35, 65, 100],
        labels=["Low", "Moderate", "High"],
    )
    risk_distribution = risk_bins.value_counts().rename_axis("Risk").reset_index(name="Count")
    risk_distribution["Risk"] = pd.Categorical(
        risk_distribution["Risk"],
        categories=["Low", "Moderate", "High"],
        ordered=True,
    )
    risk_distribution = risk_distribution.sort_values("Risk")
    risk_chart = px.bar(
        risk_distribution,
        x="Risk",
        y="Count",
        title="Training Risk Levels Compared with Your Current Risk",
        color="Risk",
        color_discrete_map={"Low": "#4d7c0f", "Moderate": "#ca8a04", "High": "#b91c1c"},
    )
    if risk:
        live_risk_level = risk["level"]
        live_count = risk_distribution.loc[risk_distribution["Risk"] == live_risk_level, "Count"]
        if not live_count.empty:
            risk_chart.add_trace(
                go.Scatter(
                    x=[live_risk_level],
                    y=[float(live_count.iloc[0])],
                    mode="markers+text",
                    marker=dict(size=16, color="#111827", symbol="diamond"),
                    text=[f"Current risk ({risk['score']})"],
                    textposition="top center",
                    name="Current input",
                )
            )
    risk_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=60, b=20),
        showlegend=False,
        xaxis_title="Risk band",
        yaxis_title="Training records",
    )

    trend_source = dataframe
    if current_input and current_input.get("crop_type"):
        trend_source = trend_source[trend_source["crop_type"] == str(current_input["crop_type"])]
    if current_input and current_input.get("region"):
        region_filtered = trend_source[trend_source["region"] == str(current_input["region"])]
        if not region_filtered.empty:
            trend_source = region_filtered
    trend_source = trend_source.sample(min(len(trend_source), 160), random_state=42)
    trend_source = trend_source.assign(
        rainfall_mm=pd.to_numeric(trend_source["rainfall_mm"], errors="coerce"),
        yield_ton_per_hectare=pd.to_numeric(trend_source["yield_ton_per_hectare"], errors="coerce"),
        humidity_pct=pd.to_numeric(trend_source["humidity_pct"], errors="coerce"),
    ).dropna(subset=["rainfall_mm", "yield_ton_per_hectare", "humidity_pct"]).sort_values("rainfall_mm")

    trend_chart = px.scatter(
        trend_source,
        x="rainfall_mm",
        y="yield_ton_per_hectare",
        color="crop_type",
        size="humidity_pct",
        hover_data=["region", "soil_type"],
        title=(
            f"Rainfall vs Yield Trend for {current_input['crop_type']} in {current_input['region']} with Your Forecast"
            if current_input and current_input.get("crop_type") and current_input.get("region")
            else "Rainfall vs Yield Trend"
        ),
        color_discrete_sequence=["#436850", "#65a30d", "#0f766e", "#f59e0b", "#b45309"],
    )
    if len(trend_source) >= 4:
        coeffs = np.polyfit(
            trend_source["rainfall_mm"].astype(float),
            trend_source["yield_ton_per_hectare"].astype(float),
            1,
        )
        fit_x = np.linspace(
            float(trend_source["rainfall_mm"].min()),
            float(trend_source["rainfall_mm"].max()),
            40,
        )
        fit_y = coeffs[0] * fit_x + coeffs[1]
        trend_chart.add_trace(
            go.Scatter(
                x=fit_x.tolist(),
                y=fit_y.tolist(),
                mode="lines",
                line=dict(color="#1f2a1e", width=3, dash="dash"),
                name="Dataset trend",
                hoverinfo="skip",
            )
        )
    trend_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis_title="Rainfall (mm)",
        yaxis_title="Yield (t/ha)",
    )
    if current_input and predicted_yield is not None:
        trend_chart.add_trace(
            go.Scatter(
                x=[float(current_input["rainfall_mm"])],
                y=[predicted_yield],
                mode="markers+text",
                marker=dict(size=18, color="#d97706", symbol="star"),
                text=["Your current input"],
                textposition="top center",
                name="Current input",
            )
        )
        trend_chart.add_vline(
            x=float(current_input["rainfall_mm"]),
            line_dash="dash",
            line_color="#d97706",
            opacity=0.6,
        )

    return {
        "comparison": comparison_chart,
        "yield_by_crop": yield_chart,
        "risk_distribution": risk_chart,
        "trend": trend_chart,
    }


def create_figures(
    dataframe: pd.DataFrame,
    model_results: list[ModelResult],
    current_input: dict[str, float | str] | None = None,
    predicted_yield: float | None = None,
    risk: dict[str, object] | None = None,
) -> dict[str, str]:
    figures = build_analytics_figures(
        dataframe,
        model_results,
        current_input=current_input,
        predicted_yield=predicted_yield,
        risk=risk,
    )

    return {
        "comparison": figures["comparison"].to_html(full_html=False, include_plotlyjs="cdn"),
        "yield_by_crop": figures["yield_by_crop"].to_html(full_html=False, include_plotlyjs=False),
        "risk_distribution": figures["risk_distribution"].to_html(full_html=False, include_plotlyjs=False),
        "trend": figures["trend"].to_html(full_html=False, include_plotlyjs=False),
    }


def build_dataset_records(dataframe: pd.DataFrame) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    preview_rows = dataframe.head(80).reset_index(drop=True)
    for index, row in preview_rows.iterrows():
        records.append(
            {
                "row_id": int(index),
                "label": (
                    f"{row['crop_type']} | {row['region']} | "
                    f"Rainfall {safe_float(row['rainfall_mm'])} | "
                    f"Yield {safe_float(row['yield_ton_per_hectare'])}"
                ),
            }
        )
    return records


def build_simple_profiles(dataframe: pd.DataFrame) -> dict[str, dict[str, dict[str, float | str]]]:
    grouped = (
        dataframe.groupby(["crop_type", "region"], as_index=False)
        .agg(
            {
                "soil_type": lambda values: values.mode().iloc[0] if not values.mode().empty else values.iloc[0],
                "rainfall_mm": "mean",
                "temperature_c": "mean",
                "humidity_pct": "mean",
                "soil_ph": "mean",
                "nitrogen_kg_ha": "mean",
                "phosphorus_kg_ha": "mean",
                "potassium_kg_ha": "mean",
                "pest_risk": "mean",
            }
        )
    )

    profiles: dict[str, dict[str, dict[str, float | str]]] = {}
    for _, row in grouped.iterrows():
        crop = str(row["crop_type"])
        region = str(row["region"])
        profiles.setdefault(crop, {})[region] = {
            "crop_type": crop,
            "region": region,
            "soil_type": str(row["soil_type"]),
            "rainfall_mm": safe_float(row["rainfall_mm"]),
            "temperature_c": safe_float(row["temperature_c"]),
            "humidity_pct": safe_float(row["humidity_pct"]),
            "soil_ph": safe_float(row["soil_ph"]),
            "nitrogen_kg_ha": safe_float(row["nitrogen_kg_ha"]),
            "phosphorus_kg_ha": safe_float(row["phosphorus_kg_ha"]),
            "potassium_kg_ha": safe_float(row["potassium_kg_ha"]),
            "pest_risk": safe_float(row["pest_risk"]),
        }
    return profiles


def build_location_profiles(dataframe: pd.DataFrame) -> dict[str, object]:
    available_regions = set(dataframe["region"].astype(str).unique().tolist())
    fallback_region = "Central" if "Central" in available_regions else sorted(available_regions)[0]
    location_profiles: dict[str, object] = {}

    for country, details in COUNTRY_LOCATION_PROFILES.items():
        directions: dict[str, object] = {}
        for direction, direction_info in details["directions"].items():
            mapped_region = direction_info["region"]
            if mapped_region not in available_regions:
                mapped_region = fallback_region

            directions[direction] = {
                "region": mapped_region,
                "summary": direction_info["summary"],
            }

        location_profiles[country] = {
            "description": details["description"],
            "adjustments": details["adjustments"],
            "directions": directions,
        }

    return location_profiles


@lru_cache(maxsize=1)
def load_project_state() -> dict[str, object]:
    init_auth_db()
    ensure_dataset(DATASET_PATH)
    active_dataset_path = get_active_dataset_path()
    dataframe = pd.read_csv(active_dataset_path)
    evaluation = evaluate_models(dataframe)
    best_model: ModelResult = evaluation["best_model"]

    dataframe = dataframe.copy()
    dataframe["risk_score"] = dataframe.apply(
        lambda row: calculate_risk(row.to_dict())["score"], axis=1
    )

    return {
        "dataset": dataframe,
        "results": evaluation["results"],
        "best_model": best_model,
        "ga_ann_improvement": evaluation["ga_ann_improvement"],
        "figures": create_figures(dataframe, evaluation["results"]),
        "active_dataset_name": active_dataset_path.name,
        "active_dataset_source": "Custom uploaded dataset" if active_dataset_path == CUSTOM_DATASET_PATH else "Built-in demo dataset",
        "dataset_records": build_dataset_records(dataframe),
        "simple_profiles": build_simple_profiles(dataframe),
        "location_profiles": build_location_profiles(dataframe),
        "season_profiles": SEASON_PROFILES,
    }


def get_form_options(dataframe: pd.DataFrame) -> dict[str, list[str]]:
    return {
        "crop_types": sorted(dataframe["crop_type"].unique().tolist()),
        "regions": sorted(dataframe["region"].unique().tolist()),
        "soil_types": sorted(dataframe["soil_type"].unique().tolist()),
    }


def build_info_page(title: str, eyebrow: str, heading: str, sections: list[dict[str, object]]) -> str:
    current_user = get_current_user()
    if not current_user:
        return redirect(url_for("login"))

    return render_template(
        "info_page.html",
        current_user=current_user,
        page_title=title,
        page_eyebrow=eyebrow,
        page_heading=heading,
        sections=sections,
    )


def build_advisory_context(current_user: dict[str, object], state: dict[str, object]) -> dict[str, object]:
    advisory_preferences = maybe_generate_advisories(current_user, state)
    advisory_notifications = list_user_notifications(current_user["id"], limit=20)
    unread_notifications = get_unread_notification_count(current_user["id"])
    advisory_popup = get_priority_popup_notification(current_user["id"])
    advisory_engine_state = get_advisory_state(current_user["id"])
    advisory_snapshot = build_advisory_snapshot(advisory_preferences, state)
    primary_recommendation = build_primary_recommendation(advisory_snapshot)

    return {
        "advisory_preferences": advisory_preferences,
        "advisory_notifications": advisory_notifications,
        "advisory_unread_count": unread_notifications,
        "advisory_popup": advisory_popup,
        "advisory_engine_state": advisory_engine_state,
        "primary_recommendation": primary_recommendation,
        "smtp_status": get_smtp_status(),
        "smtp_ready": can_send_email_alerts(),
    }


@app.route("/")
def index():
    current_user = get_current_user()
    if not current_user:
        return redirect(url_for("login"))
    touch_user_activity(current_user["id"])

    state = load_project_state()
    dataset = state["dataset"]
    best_model: ModelResult = state["best_model"]
    results: list[ModelResult] = state["results"]

    latest_rows = (
        dataset.sort_values(["crop_type", "yield_ton_per_hectare"], ascending=[True, False])
        .head(15)
        .to_dict(orient="records")
    )

    summary = {
        "dataset_rows": int(len(dataset)),
        "crop_count": int(dataset["crop_type"].nunique()),
        "avg_yield": safe_float(dataset["yield_ton_per_hectare"].mean()),
        "best_model": best_model.name,
        "best_r2": best_model.r2,
        "best_rmse": best_model.rmse,
        "best_mae": best_model.mae,
        "best_confidence": safe_float(best_model.r2 * 100),
        "dataset_name": state["active_dataset_name"],
        "dataset_source": state["active_dataset_source"],
    }
    model_use_cases = {
        "Linear Regression": "Best for simple linear relationships and easy baseline interpretation.",
        "Random Forest": "Best for strong general-purpose prediction on mixed agricultural features.",
        "Gradient Boosting": "Best when high prediction accuracy is needed from complex nonlinear patterns.",
        "GA-Optimized ANN": "Best for nonlinear pattern learning after ANN settings are tuned through GA-style optimization.",
        "Fuzzy Logic Risk Engine": "Best for interpreting field uncertainty and converting stress factors into Low, Moderate, or High risk.",
    }
    if best_model.name in {"Gradient Boosting", "Random Forest"}:
        comparison_insight = (
            "Tree-based ensemble models are leading this dataset because agricultural yield patterns are nonlinear "
            "and benefit from models that handle interacting climate and soil variables."
        )
    elif best_model.name == "GA-Optimized ANN":
        comparison_insight = (
            "The tuned GA-Optimized ANN is performing strongly here because the dataset contains nonlinear "
            "patterns that benefit from a tuned neural architecture."
        )
    else:
        comparison_insight = (
            f"{best_model.name} is currently leading because it offers the most stable balance between "
            "accuracy and prediction error on validation data."
        )
    metric_meaning = (
        "Lower RMSE and MAE mean predictions stay closer to real crop yield. "
        "Higher R2 means the model explains more of the yield variation."
    )
    confidence_note = (
        "Confidence indicates model stability across validation data. Higher confidence means more reliable predictions."
    )
    ann_note = (
        "GA-Optimized ANN is tuned before evaluation, but neural networks still usually need larger datasets than tree ensembles to outperform consistently."
    )
    fuzzy_engine = {
        "name": "Fuzzy Logic Risk Engine",
        "role": "Used for interpretability and risk classification, not for yield prediction.",
        "reason": "It converts rainfall, temperature, soil, and pest uncertainty into a smoother Low / Moderate / High risk evaluation.",
    }
    advisory_context = build_advisory_context(current_user, state)

    return render_template(
        "index.html",
        summary=summary,
        model_results=results,
        figures=state["figures"],
        options=get_form_options(dataset),
        latest_rows=latest_rows,
        current_user=current_user,
        dataset_records=state["dataset_records"],
        simple_profiles=state["simple_profiles"],
        location_profiles=state["location_profiles"],
        season_profiles=state["season_profiles"],
        comparison_insight=comparison_insight,
        metric_meaning=metric_meaning,
        confidence_note=confidence_note,
        ann_note=ann_note,
        model_use_cases=model_use_cases,
        fuzzy_engine=fuzzy_engine,
        ga_ann_improvement=state["ga_ann_improvement"],
        **advisory_context,
    )


@app.get("/advisory")
def advisory_center():
    current_user = get_current_user()
    if not current_user:
        return redirect(url_for("login"))
    touch_user_activity(current_user["id"])

    state = load_project_state()
    advisory_context = build_advisory_context(current_user, state)

    return render_template(
        "advisory.html",
        current_user=current_user,
        options=get_form_options(state["dataset"]),
        **advisory_context,
    )


@app.get("/contact")
def contact():
    current_user = get_current_user()
    if not current_user:
        return redirect(url_for("login"))

    contacts = [
        {
            "role": "Team Lead",
            "name": "Sanchit Kumar",
            "email": "2023a1r118@mietjammu.in",
            "phone": "+91 95964 15537",
            "address": "Jammu, J&K",
            "institution": "MIET, Jammu",
            "program": "B.Tech in CSE",
        },
        {
            "role": "Team Member",
            "name": "Piyush Malthonia",
            "email": "2023a1r074@mietjammu.in",
            "phone": "+91 95966 31270",
            "address": "Jammu, J&K",
            "institution": "MIET, Jammu",
            "program": "B.Tech in CSE",
        },
    ]

    return render_template("contact.html", current_user=current_user, contacts=contacts)


@app.get("/about")
def about():
    return build_info_page(
        title="About Project",
        eyebrow="Project Resources",
        heading="About the Crop Yield Prediction System",
        sections=[
            {
                "title": "Project Overview",
                "paragraphs": [
                    "This system predicts crop yield from environmental and soil conditions, evaluates field risk, and presents model-backed recommendations through a user-friendly interface.",
                    "It is designed as an academic mini-project that combines machine learning, agricultural input analysis, and frontend visualization in one working application.",
                ],
            },
            {
                "title": "Why It Matters",
                "paragraphs": [
                    "Traditional yield estimation can be manual and uncertain. This project gives a faster, data-driven way to estimate likely output and flag risky conditions before planning decisions are made.",
                    "The overall goal is to support smarter crop planning, better awareness, and clearer presentation of agricultural insights.",
                ],
            },
        ],
    )


@app.get("/model-overview")
def model_overview():
    return build_info_page(
        title="Model Overview",
        eyebrow="Project Resources",
        heading="Machine learning model overview",
        sections=[
            {
                "title": "Models Used",
                "paragraphs": [
                    "The application trains and compares Linear Regression and Random Forest on the agricultural dataset.",
                    "Linear Regression serves as a simple baseline, while Random Forest captures more complex relationships between rainfall, temperature, soil, nutrients, and yield.",
                ],
            },
            {
                "title": "Model Selection",
                "paragraphs": [
                    "Model performance is compared using RMSE, MAE, and R2 Score.",
                    "The best-performing model is selected for live prediction so the output shown in the interface stays aligned with the strongest available trained result.",
                ],
            },
        ],
    )


@app.get("/risk-scoring")
def risk_scoring():
    return build_info_page(
        title="Risk Scoring",
        eyebrow="Project Resources",
        heading="How risk scoring works",
        sections=[
            {
                "title": "Risk Formula",
                "paragraphs": [
                    "The risk score is calculated using a weighted combination of rainfall deviation, temperature stress, soil condition, and pest pressure.",
                    "This turns multiple agricultural factors into one understandable indicator for crop suitability and field stress.",
                ],
            },
            {
                "title": "Risk Output",
                "paragraphs": [
                    "After calculation, the score is classified into Low, Moderate, or High risk.",
                    "This helps users understand whether the current conditions are favorable, manageable with care, or likely to reduce yield significantly.",
                ],
            },
        ],
    )


@app.get("/dataset-notes")
def dataset_notes():
    return build_info_page(
        title="Dataset Notes",
        eyebrow="Project Resources",
        heading="Training dataset notes",
        sections=[
            {
                "title": "Dataset Structure",
                "paragraphs": [
                    "The dataset includes crop type, region, soil type, rainfall, temperature, humidity, soil pH, nitrogen, phosphorus, potassium, pest risk, and yield.",
                    "These fields are used to train the machine learning models and support prediction, risk analysis, and recommendation features.",
                ],
            },
            {
                "title": "Preparation",
                "paragraphs": [
                    "Before training, the data is cleaned, encoded, normalized where needed, and organized into model-ready features.",
                    "This improves consistency and helps the deployed predictor behave more reliably during live input testing.",
                ],
            },
        ],
    )


@app.get("/faq")
def faq():
    return build_info_page(
        title="Frequently Asked Questions",
        eyebrow="Support",
        heading="Frequently Asked Questions",
        sections=[
            {
                "title": "FAQ",
                "faq_items": [
                    {
                        "question": "How does the prediction work?",
                        "answer": "The system uses a trained machine learning model to estimate crop yield based on your input conditions.",
                    },
                    {
                        "question": "What inputs are required?",
                        "answer": "You can either enter detailed parameters manually or use the farmer-friendly mode for simplified inputs.",
                    },
                    {
                        "question": "What is risk score?",
                        "answer": "It indicates how suitable the given conditions are for crop growth.",
                    },
                    {
                        "question": "Can I upload my own dataset?",
                        "answer": "Yes, the system allows uploading a CSV dataset to retrain the model.",
                    },
                    {
                        "question": "Is this system accurate?",
                        "answer": "The model provides reasonably accurate predictions based on available data, but results may vary in real-world conditions.",
                    },
                ],
            }
        ],
    )


@app.get("/privacy")
def privacy():
    return build_info_page(
        title="Privacy Policy",
        eyebrow="Support",
        heading="Privacy Policy",
        sections=[
            {
                "title": "Privacy Commitment",
                "paragraphs": [
                    "We respect your privacy and ensure that your data is handled securely.",
                    "User data such as login credentials is stored securely.",
                    "No personal data is shared with third parties.",
                    "Uploaded datasets are used only for model training within the system.",
                    "By using this application, you agree to the storage and processing of your data for system functionality.",
                ],
            }
        ],
    )


@app.get("/terms")
def terms():
    return build_info_page(
        title="Terms of Use",
        eyebrow="Support",
        heading="Terms of Use",
        sections=[
            {
                "title": "Usage Terms",
                "paragraphs": [
                    "By using this application, you agree to the following terms:",
                    "The predictions are for educational and informational purposes only.",
                    "The system does not guarantee real-world agricultural outcomes.",
                    "Users are responsible for their decisions based on the predictions.",
                    "Misuse of the platform or uploading invalid data may affect system performance.",
                ],
            }
        ],
    )


@app.get("/help")
def help_center():
    return build_info_page(
        title="Help Center",
        eyebrow="Support",
        heading="Help Center",
        sections=[
            {
                "title": "How to Use the System",
                "bullet_points": [
                    "Register or log in to your account",
                    "Enter input parameters or use farmer-friendly mode",
                    "Click on \"Predict Yield\"",
                    "View prediction, risk score, and recommendations",
                ],
            },
            {
                "title": "If Issues Persist",
                "bullet_points": [
                    "Check input values",
                    "Ensure dataset format is correct",
                    "Refresh the application",
                    "For further help, contact the development team using Contact Details tab at the bottom",
                ],
            },
        ],
    )


@app.route("/signup", methods=["GET", "POST"])
def signup():
    init_auth_db()
    if get_current_user():
        return redirect(url_for("index"))

    error = None

    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")
        email_alerts_enabled = boolify(request.form.get("email_alerts_enabled", "1"))

        if not full_name or not email or not password:
            error = "Please fill in all required fields."
        elif password != confirm_password:
            error = "Passwords do not match."
        elif len(password) < 6:
            error = "Password must be at least 6 characters long."
        else:
            connection = get_db_connection()
            try:
                with connection.cursor() as cursor:
                    run_query(
                        cursor,
                        "SELECT id FROM users WHERE email = %s",
                        (email,),
                    )
                    existing_user = cursor.fetchone()
                if existing_user:
                    error = "An account with this email already exists."
                else:
                    with connection.cursor() as cursor:
                        if ACTIVE_DB_BACKEND == "postgres":
                            run_query(
                                cursor,
                                "INSERT INTO users (full_name, email, password_hash) VALUES (%s, %s, %s) RETURNING id",
                                (full_name, email, generate_password_hash(password)),
                            )
                            created_user = row_to_dict(cursor.fetchone())
                        else:
                            run_query(
                                cursor,
                                "INSERT INTO users (full_name, email, password_hash) VALUES (%s, %s, %s)",
                                (full_name, email, generate_password_hash(password)),
                            )
                            created_user = {"id": cursor.lastrowid}
                    connection.commit()
                    get_or_create_advisory_preferences(
                        created_user["id"],
                        overrides={
                            "email_alerts_enabled": email_alerts_enabled,
                            "in_app_alerts_enabled": True,
                        },
                    )
                    session["user_id"] = created_user["id"]
                    touch_user_activity(created_user["id"], login=True)
                    return redirect(url_for("index"))
            finally:
                connection.close()

    return render_template("signup.html", error=error)


@app.route("/login", methods=["GET", "POST"])
def login():
    init_auth_db()
    if get_current_user():
        return redirect(url_for("index"))

    error = None

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        connection = get_db_connection()
        try:
            with connection.cursor() as cursor:
                run_query(
                    cursor,
                    "SELECT id, full_name, email, password_hash FROM users WHERE email = %s",
                    (email,),
                )
                user = row_to_dict(cursor.fetchone())
        finally:
            connection.close()

        if not user or not check_password_hash(user["password_hash"], password):
            error = "Invalid email or password."
        else:
            session["user_id"] = user["id"]
            touch_user_activity(user["id"], login=True)
            return redirect(url_for("index"))

    return render_template("login.html", error=error)


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    init_auth_db()
    if get_current_user():
        return redirect(url_for("index"))

    error = None
    success = None

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        new_password = request.form.get("new_password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not email or not new_password or not confirm_password:
            error = "Please fill in all required fields."
        elif new_password != confirm_password:
            error = "Passwords do not match."
        elif len(new_password) < 6:
            error = "Password must be at least 6 characters long."
        else:
            connection = get_db_connection()
            try:
                with connection.cursor() as cursor:
                    run_query(
                        cursor,
                        "SELECT id FROM users WHERE email = %s",
                        (email,),
                    )
                    user = cursor.fetchone()
                if not user:
                    error = "No account found with this email address."
                else:
                    with connection.cursor() as cursor:
                        run_query(
                            cursor,
                            "UPDATE users SET password_hash = %s WHERE email = %s",
                            (generate_password_hash(new_password), email),
                        )
                    connection.commit()
                    success = "Password updated successfully. You can now log in with the new password."
            finally:
                connection.close()

    return render_template("forgot_password.html", error=error, success=success)


@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.post("/api/upload-dataset")
def upload_dataset():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    uploaded_file = request.files.get("dataset_file")
    if not uploaded_file or not uploaded_file.filename:
        return jsonify({"error": "Please choose a CSV file to upload."}), 400

    if not uploaded_file.filename.lower().endswith(".csv"):
        return jsonify({"error": "Only CSV files are supported."}), 400

    try:
        file_text = uploaded_file.read().decode("utf-8")
        dataframe = pd.read_csv(StringIO(file_text))
    except Exception:
        return jsonify({"error": "The uploaded file could not be read as a valid CSV."}), 400

    is_valid, error_message = validate_dataset(dataframe)
    if not is_valid:
        return jsonify({"error": error_message}), 400

    CUSTOM_DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(CUSTOM_DATASET_PATH, index=False)
    load_project_state.cache_clear()
    state = load_project_state()

    return jsonify(
        {
            "message": "Dataset uploaded successfully. Models retrained using the new backend dataset.",
            "dataset_name": state["active_dataset_name"],
            "dataset_source": state["active_dataset_source"],
            "dataset_rows": len(state["dataset"]),
            "best_model": state["best_model"].name,
        }
    )


@app.post("/api/reset-dataset")
def reset_dataset():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    if CUSTOM_DATASET_PATH.exists():
        CUSTOM_DATASET_PATH.unlink()
    load_project_state.cache_clear()
    state = load_project_state()
    return jsonify(
        {
            "message": "Dataset reset to the built-in demo dataset.",
            "dataset_name": state["active_dataset_name"],
            "dataset_source": state["active_dataset_source"],
            "dataset_rows": len(state["dataset"]),
            "best_model": state["best_model"].name,
        }
    )


@app.post("/api/predict")
def predict():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    state = load_project_state()
    best_model: ModelResult = state["best_model"]

    payload = request.get_json(force=True)
    validation_errors = validate_prediction_inputs(payload)
    if validation_errors:
        return jsonify({"error": "Please correct the highlighted inputs.", "validation_errors": validation_errors}), 400

    input_frame = pd.DataFrame([payload], columns=FEATURE_COLUMNS)
    predicted_yield = safe_float(best_model.pipeline.predict(input_frame)[0])
    risk = calculate_risk(payload)
    recommendations = build_recommendations(payload, risk)
    confidence = calculate_confidence(state["dataset"], payload, best_model)
    action_cards = build_action_cards(payload, risk)
    insights = build_insights(payload, risk, state["dataset"])
    recommended_crop = recommend_crop_for_inputs(payload, state)
    season_message = build_season_message(payload)
    ai_statement = build_ai_statement(payload, risk, predicted_yield)

    dataset_context = None
    merged_yield = predicted_yield
    merged_note = "Live output is using the trained model only."
    selected_row_id = payload.get("dataset_row_id")
    if selected_row_id not in (None, "", "null"):
        try:
            row_index = int(selected_row_id)
            dataset = state["dataset"].reset_index(drop=True)
            if 0 <= row_index < len(dataset):
                dataset_row = dataset.iloc[row_index]
                actual_yield = safe_float(dataset_row["yield_ton_per_hectare"])
                merged_yield = safe_float(predicted_yield * 0.7 + actual_yield * 0.3)
                difference = safe_float(predicted_yield - actual_yield)
                dataset_context = {
                    "row_id": row_index,
                    "crop_type": str(dataset_row["crop_type"]),
                    "region": str(dataset_row["region"]),
                    "soil_type": str(dataset_row["soil_type"]),
                    "actual_yield": actual_yield,
                    "difference": difference,
                }
                merged_note = (
                    "Merged output combines 70% live model prediction with 30% selected "
                    "dataset yield to keep the result grounded in your CSV record."
                )
        except (ValueError, TypeError):
            dataset_context = None

    response = {
        "predicted_yield": predicted_yield,
        "merged_yield": merged_yield,
        "merged_note": merged_note,
        "yield_band": (
            "High Potential"
            if predicted_yield >= 6.5
            else "Stable Potential"
            if predicted_yield >= 4.5
            else "Needs Attention"
        ),
        "best_model": best_model.name,
        "best_model_metrics": {
            "r2": best_model.r2,
            "rmse": best_model.rmse,
            "mae": best_model.mae,
            "rank": best_model.rank,
        },
        "risk": risk,
        "confidence": confidence,
        "insights": insights,
        "action_cards": action_cards,
        "recommendations": recommendations,
        "recommended_crop": recommended_crop,
        "season_message": season_message,
        "ai_statement": ai_statement,
        "ga_ann_improvement": state["ga_ann_improvement"],
        "dataset_context": dataset_context,
        "input_summary": {
            "crop_type": payload["crop_type"],
            "region": payload["region"],
            "soil_type": payload["soil_type"],
            "season": payload.get("season"),
        },
    }

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                INSERT INTO predictions (
                    user_id,
                    crop,
                    yield_value,
                    risk,
                    crop_type,
                    region,
                    predicted_yield,
                    risk_level
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session["user_id"],
                    payload["crop_type"],
                    predicted_yield,
                    risk["level"],
                    payload["crop_type"],
                    payload.get("region"),
                    predicted_yield,
                    risk["level"],
                ),
            )
        connection.commit()
    finally:
        connection.close()

    return jsonify(response)


@app.post("/api/compare-scenarios")
def compare_scenarios():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    state = load_project_state()
    best_model: ModelResult = state["best_model"]
    payload = request.get_json(force=True) or {}
    base_input = payload.get("current_input") or {}
    improved_input = payload.get("improved_input") or {}

    if not base_input:
        return jsonify({"error": "A current scenario payload is required."}), 400

    improvement_notes: list[str] = []
    if not improved_input:
        improved_input, improvement_notes = build_improved_management_scenario(
            state["dataset"], base_input
        )

    def evaluate_scenario(inputs: dict[str, object]) -> dict[str, object]:
        validation_errors = validate_prediction_inputs(inputs)
        if validation_errors:
            return {"error": validation_errors}

        input_frame = pd.DataFrame([inputs], columns=FEATURE_COLUMNS)
        predicted = safe_float(best_model.pipeline.predict(input_frame)[0])
        risk = calculate_risk(inputs)
        confidence = calculate_confidence(state["dataset"], inputs, best_model)
        return {
            "predicted_yield": predicted,
            "risk": risk,
            "confidence": confidence,
            "crop_type": inputs["crop_type"],
            "region": inputs["region"],
            "season": inputs.get("season"),
        }

    current_result = evaluate_scenario(base_input)
    improved_result = evaluate_scenario(improved_input)
    if current_result.get("error") or improved_result.get("error"):
        return jsonify({"error": "Scenario inputs are invalid.", "details": [current_result, improved_result]}), 400

    if improved_result["confidence"]["score"] < current_result["confidence"]["score"]:
        improved_result["confidence"]["score"] = current_result["confidence"]["score"]
        improved_result["confidence"]["label"] = current_result["confidence"]["label"]

    improvement = safe_float(improved_result["predicted_yield"] - current_result["predicted_yield"])
    risk_change = safe_float(current_result["risk"]["score"] - improved_result["risk"]["score"])

    return jsonify(
        {
            "current": current_result,
            "improved": improved_result,
            "yield_gain": improvement,
            "risk_reduction": risk_change,
            "improvement_notes": improvement_notes,
        }
    )


@app.post("/api/export-report")
def export_report():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(force=True) or {}
    report_data = payload.get("report_data") or {}
    if not report_data:
        return jsonify({"error": "No report data was provided."}), 400

    html = build_report_html(report_data)
    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": "attachment; filename=crop_yield_report.html"},
    )


@app.post("/api/context-figures")
def context_figures():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    state = load_project_state()
    payload = request.get_json(force=True) or {}
    current_input = payload.get("current_input") or {}
    predicted_yield = payload.get("predicted_yield")
    risk = payload.get("risk")

    figures = build_analytics_figures(
        state["dataset"],
        state["results"],
        current_input=current_input if current_input else None,
        predicted_yield=float(predicted_yield) if predicted_yield is not None else None,
        risk=risk if isinstance(risk, dict) else None,
    )

    return Response(
        json.dumps(
            {
                "yield_by_crop": figures["yield_by_crop"].to_plotly_json(),
                "risk_distribution": figures["risk_distribution"].to_plotly_json(),
                "trend": figures["trend"].to_plotly_json(),
            },
            cls=PlotlyJSONEncoder,
        ),
        mimetype="application/json",
    )


@app.post("/api/recommend-crop")
def recommend_crop():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    state = load_project_state()
    payload = request.get_json(force=True)
    validation_errors = validate_prediction_inputs(payload)
    if validation_errors:
        return jsonify({"error": "Please correct the highlighted inputs.", "validation_errors": validation_errors}), 400
    ranked_matches = rank_crop_predictions(state, payload)
    best_crop = ranked_matches[0]
    return jsonify(
        {
            "recommended_crop": best_crop["crop"],
            "expected_yield": best_crop["predicted_yield"],
            "match_score": best_crop["match_score"],
            "fit_label": best_crop["fit_label"],
            "risk_level": best_crop["risk_level"],
            "risk_score": best_crop["risk_score"],
            "suitability_score": best_crop["suitability_score"],
            "all_predictions": ranked_matches,
            "score_guide": [
                {"label": "Excellent fit", "range": "0.00 - 0.18", "meaning": "Very close to ideal crop conditions"},
                {"label": "Good fit", "range": "0.19 - 0.32", "meaning": "Suitable with only small adjustments needed"},
                {"label": "Moderate fit", "range": "0.33 - 0.50", "meaning": "Can work but needs careful management"},
                {"label": "Weak fit", "range": "Above 0.50", "meaning": "Conditions are far from ideal for this crop"},
            ],
            "score_note": "Recommendation is ranked by best crop fit first, then lower risk, and then projected yield so the top result stays easier to trust in real field conditions.",
        }
    )


@app.get("/api/history")
def get_history():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            run_query(
                cursor,
                """
                SELECT
                    COALESCE(crop_type, crop) AS crop_name,
                    COALESCE(region, 'Not provided') AS region_name,
                    COALESCE(predicted_yield, yield_value) AS predicted_value,
                    COALESCE(risk_level, risk) AS risk_name,
                    created_at
                FROM predictions
                WHERE user_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 10
                """,
                (session["user_id"],),
            )
            rows = cursor.fetchall()
    finally:
        connection.close()

    history = [
        {
            "crop": row["crop_name"] or "Unknown crop",
            "region": row["region_name"] or "Not provided",
            "yield": row["predicted_value"],
            "risk": row["risk_name"] or "Unknown",
            "time": str(row["created_at"]),
        }
        for row in rows
    ]
    return jsonify(history)


@app.get("/api/notifications")
def get_notifications():
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    category = request.args.get("category")
    severity = request.args.get("severity")
    notifications = list_user_notifications(current_user["id"], category=category, severity=severity, limit=20)
    preferences = get_or_create_advisory_preferences(current_user["id"])
    state = load_project_state()
    primary_recommendation = build_primary_recommendation(build_advisory_snapshot(preferences, state))
    return jsonify(
        {
            "notifications": notifications,
            "unread_count": get_unread_notification_count(current_user["id"]),
            "preferences": preferences,
            "priority_popup": get_priority_popup_notification(current_user["id"]),
            "engine_state": get_advisory_state(current_user["id"]),
            "main_recommendation": primary_recommendation,
            "smtp_status": get_smtp_status(),
            "smtp_ready": can_send_email_alerts(),
        }
    )


@app.post("/api/notifications/refresh")
def refresh_notifications():
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    state = load_project_state()
    preferences = maybe_generate_advisories(current_user, state, force=True)
    return jsonify(
        {
            "notifications": list_user_notifications(current_user["id"], limit=20),
            "unread_count": get_unread_notification_count(current_user["id"]),
            "preferences": preferences,
            "priority_popup": get_priority_popup_notification(current_user["id"]),
            "engine_state": get_advisory_state(current_user["id"]),
            "main_recommendation": build_primary_recommendation(build_advisory_snapshot(preferences, state)),
            "smtp_status": get_smtp_status(),
            "smtp_ready": can_send_email_alerts(),
        }
    )


@app.post("/api/notifications/send-email")
def send_notification_email_now():
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    if not can_send_email_alerts():
        smtp_status = get_smtp_status()
        missing = ", ".join(smtp_status["missing_required"]) or "SMTP settings"
        return jsonify({"error": f"Email is not configured yet. Missing: {missing}."}), 400

    state = load_project_state()
    preferences = maybe_generate_advisories(current_user, state, force=True)
    notifications = list_user_notifications(current_user["id"], limit=20)
    email_candidates = [
        item for item in notifications
        if severity_rank(item.get("priority")) >= 1 or (item.get("ui_tone") == "opportunity")
    ]
    if not email_candidates:
        return jsonify({"error": "No advisory update is available to send yet."}), 400
    if was_alert_emailed_recently(current_user["id"], str(email_candidates[0].get("signature") or ""), hours=24):
        return jsonify({"error": "This latest alert was already emailed recently."}), 400

    email_ok, email_status = send_advisory_email(current_user, [email_candidates[0]])
    if not email_ok:
        return jsonify({"error": f"Email could not be sent: {email_status}"}), 400

    log_alert_events(current_user["id"], [email_candidates[0]], channel="email_manual", status="sent")
    return jsonify(
        {
            "ok": True,
            "message": f"Latest advisory sent to {current_user['email']}.",
            "state": {
                "notifications": notifications,
                "unread_count": get_unread_notification_count(current_user["id"]),
                "preferences": preferences,
                "priority_popup": get_priority_popup_notification(current_user["id"]),
                "engine_state": get_advisory_state(current_user["id"]),
                "main_recommendation": build_primary_recommendation(build_advisory_snapshot(preferences, state)),
                "smtp_status": get_smtp_status(),
                "smtp_ready": can_send_email_alerts(),
            },
        }
    )


@app.post("/send-latest")
def send_latest_email():
    return send_notification_email_now()


@app.get("/test-email")
def test_email():
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    preferences = get_or_create_advisory_preferences(current_user["id"])
    if not preferences.get("email_alerts_enabled"):
        return jsonify({"error": "Email alerts are disabled for this account."}), 400

    ok, status = send_email(
        current_user["email"],
        "Test Email",
        "Your crop alert system is working ✅",
    )
    if not ok:
        return jsonify({"error": f"Test email could not be sent: {status}"}), 400
    return jsonify({"ok": True, "message": f"Test email sent to {current_user['email']}."})


@app.post("/api/notifications/<int:notification_id>/read")
def read_notification(notification_id: int):
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    dismiss_popup = boolify((request.get_json(silent=True) or {}).get("dismiss_popup", "0"))
    if not mark_notification_as_read(current_user["id"], notification_id, dismiss_popup=dismiss_popup):
        return jsonify({"error": "Notification not found"}), 404

    return jsonify({"ok": True, "unread_count": get_unread_notification_count(current_user["id"])})


@app.post("/api/notifications/<int:notification_id>/dismiss")
def dismiss_notification(notification_id: int):
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    if not dismiss_notification_popup(current_user["id"], notification_id):
        return jsonify({"error": "Notification not found"}), 404

    return jsonify({"ok": True})


@app.post("/api/advisory-preferences")
def save_advisory_preferences():
    current_user = get_current_user()
    if not current_user:
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or request.form.to_dict()
    state = load_project_state()
    preferences = update_advisory_preferences(current_user["id"], payload, state=state)
    return jsonify(
        {
            "ok": True,
            "preferences": preferences,
            "engine_state": get_advisory_state(current_user["id"]),
            "smtp_status": get_smtp_status(),
            "smtp_ready": can_send_email_alerts(),
        }
    )


@app.post("/internal/run-advisories")
def run_advisories_internal():
    admin_token = os.getenv("ADVISORY_CRON_TOKEN", "")
    request_token = request.headers.get("X-Advisory-Token", "")
    if not admin_token or request_token != admin_token:
        return jsonify({"error": "Forbidden"}), 403

    processed = run_advisory_engine_for_all_users(force=True)
    return jsonify({"ok": True, "processed_users": processed})


ADVISORY_SCHEDULER = None

init_auth_db()


def start_advisory_scheduler() -> None:
    global ADVISORY_SCHEDULER
    if ADVISORY_SCHEDULER is not None or BackgroundScheduler is None:
        return
    if not boolify(os.getenv("RUN_ADVISORY_SCHEDULER", "0")):
        return

    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        lambda: run_advisory_engine_for_all_users(force=False),
        "interval",
        hours=max(24, int(os.getenv("ADVISORY_SCAN_HOURS", "24"))),
        id="daily_smart_advisory_cycle",
        replace_existing=True,
    )
    scheduler.start()
    ADVISORY_SCHEDULER = scheduler


@app.route("/api/dataset-preview")
def dataset_preview():
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    dataset = load_project_state()["dataset"]
    preview = dataset.head(20).to_dict(orient="records")
    return app.response_class(
        response=json.dumps(preview, indent=2),
        status=200,
        mimetype="application/json",
    )


@app.get("/api/dataset-row/<int:row_id>")
def dataset_row(row_id: int):
    if not get_current_user():
        return jsonify({"error": "Unauthorized"}), 401

    dataset = load_project_state()["dataset"].reset_index(drop=True)
    if row_id < 0 or row_id >= len(dataset):
        return jsonify({"error": "Dataset row not found."}), 404

    row = dataset.iloc[row_id].to_dict()
    row["row_id"] = row_id
    return jsonify(row)


start_advisory_scheduler()


if __name__ == "__main__":
    app.run(
        debug=False,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
    )
