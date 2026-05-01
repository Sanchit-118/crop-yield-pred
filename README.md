# Crop Yield Prediction System

This is a working project website with backend, live prediction logic, dataset generation, model comparison, risk scoring, and presentation-ready material for academic demonstration.

## What this project includes

- Flask backend for routing and prediction
- Dataset-driven model training
- Linear Regression and Random Forest comparison
- Live crop yield prediction form
- Yield risk scoring with weighted factors
- Plotly charts for analytics
- Project steps and implementation explanation for viva or invigilator review
- Exported `.pkl` model files for deployment or reuse

## Technology stack

- Python
- Flask
- pandas
- numpy
- scikit-learn
- plotly
- HTML, CSS, JavaScript

## Project structure

```text
app.py
generate_dataset.py
export_models.py
data/crop_yield_dataset.csv
models/best_model.pkl
models/linear_regression.pkl
models/random_forest.pkl
templates/index.html
static/style.css
static/app.js
README.md
IMPLEMENTATION.md
```

## How the system works

1. The dataset is generated in `data/crop_yield_dataset.csv` if it does not already exist.
2. The backend loads the dataset and splits it into training and testing sets.
3. Two regression models are trained:
   - Linear Regression
   - Random Forest Regressor
4. The models are evaluated using:
   - RMSE
   - MAE
   - R2 Score
5. The model with the best `R2 Score` is used for live predictions.
6. A weighted risk score is calculated using:
   - Rainfall deviation
   - Temperature stress
   - Soil condition
   - Pest risk
7. The frontend sends user inputs to `/api/predict` and displays the results instantly.

## How to run the project

1. Open PowerShell in this project folder.
2. Create a local environment file once:

```text
Copy .env.example to .env
```

3. For local development, keep `.env` simple:

```powershell
APP_DB_MODE=sqlite
```

You can also add:

```powershell
SECRET_KEY=any-random-secret
```

4. Run:

```powershell
python generate_dataset.py
python export_models.py
python app.py
```

Or simply run:

```powershell
run_app.bat
```

5. Open this URL in your browser:

```text
http://127.0.0.1:5000
```

After you create `.env` once, you do not need to set anything again in every new PowerShell window.

If you explicitly want local PostgreSQL later, set:

```powershell
APP_DB_MODE=postgres
DATABASE_URL=postgresql://username:password@host:5432/database_name?sslmode=require
```

## Deploy on Render

Use a `Web Service` with these settings:

- Build Command: `pip install -r requirements.txt`
- Start Command: `gunicorn app:app`

Recommended environment variables:

- `SECRET_KEY` = any long random string
- `DATABASE_URL` = your PostgreSQL connection string
- `PYTHON_VERSION` = `3.13.7` if you prefer setting it in Render instead of using `.python-version`

## GitHub-ready files

- `.gitignore` keeps local-only folders out of version control
- `requirements.txt` lists Python dependencies
- `Procfile` is included for simple web deployment platforms
- `runtime.txt` pins the Python version
- `models/` stores exported `.pkl` model artifacts

## Useful demo points for presentation

- Explain that the system compares two machine learning models before selecting the best one.
- Show that the website is not static because the result changes for each new input.
- Highlight that the risk score is separate from the yield prediction and adds decision support.
- Use the charts section to discuss trends in crop performance and risk distribution.
- Open `IMPLEMENTATION.md` if you need a structured explanation of the code and workflow.
