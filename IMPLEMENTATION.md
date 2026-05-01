# Implementation Notes

## Step-by-step workflow

### 1. Dataset preparation

The file `generate_dataset.py` creates `data/crop_yield_dataset.csv` with crop, region, soil, climate, nutrient, pest, and yield columns.

Main fields:

- `crop_type`
- `region`
- `soil_type`
- `rainfall_mm`
- `temperature_c`
- `humidity_pct`
- `soil_ph`
- `nitrogen_kg_ha`
- `phosphorus_kg_ha`
- `potassium_kg_ha`
- `pest_risk`
- `yield_ton_per_hectare`

### 2. Preprocessing

In `app.py`, preprocessing is done using `ColumnTransformer`:

- Categorical columns are handled by `OneHotEncoder`
- Numerical columns are handled by `StandardScaler`

This keeps the data ready for machine learning pipelines.

### 3. Model training

Two models are trained:

- `LinearRegression()`
- `RandomForestRegressor(...)`

The dataset is split using `train_test_split`, and each model is evaluated on test data.

### 4. Evaluation metrics

The application computes:

- `RMSE`
- `MAE`
- `R2 Score`

The model with the highest `R2 Score` is selected as the best model for prediction.

### 5. Yield risk scoring

Risk is computed with this formula:

```text
Risk Score = Sum(Weight * Factor Score)
```

Risk factors used:

- Rainfall deviation
- Temperature stress
- Soil condition
- Pest risk

Risk categories:

- Low
- Moderate
- High

### 6. Backend routes

The Flask backend exposes these key routes:

- `/` renders the full dashboard page
- `/api/predict` accepts form data and returns predicted yield and risk details
- `/api/dataset-preview` returns sample dataset rows in JSON format

### 7. Frontend behavior

The frontend:

- shows model metrics
- renders charts
- submits user input using JavaScript `fetch`
- updates the result card without reloading the page

## Important code sections

### Model evaluation

Located in `evaluate_models()` inside `app.py`.

### Risk calculation

Located in `calculate_risk()` inside `app.py`.

### Recommendations

Located in `build_recommendations()` inside `app.py`.

### Dataset generation

Located in `ensure_dataset()` inside `generate_dataset.py`.

## How to explain this to an invigilator

You can present the project in this order:

1. State the agriculture problem and why manual prediction is unreliable.
2. Show the dataset columns and explain the factors used for prediction.
3. Explain the preprocessing and why categorical and numerical features are treated differently.
4. Compare Linear Regression and Random Forest using RMSE, MAE, and R2 score.
5. Demonstrate live prediction on the website.
6. Explain the risk score formula and how it helps farmers beyond just yield value.
7. Show the charts and dataset preview to support the model output visually.
