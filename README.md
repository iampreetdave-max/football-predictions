# Football Predictions

A football match-prediction system that fetches the day's fixtures, engineers pre-match features, and uses trained Ridge regression models to forecast scorelines, outcomes, over/under, and BTTS — all driven from a Streamlit control panel.

![Python](https://img.shields.io/badge/python-3.x-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-app-FF4B4B)
![scikit-learn](https://img.shields.io/badge/scikit--learn-Ridge-F7931E)

## Overview

This project predicts football match results from pre-match statistics. It is organized as a three-stage pipeline — fetch fixtures, build features, predict — with a Streamlit app that orchestrates each stage, shows the intermediate CSV outputs, and surfaces summary metrics and high-confidence picks.

Predictions come from two trained Ridge regression models (home goals and away goals) applied to a weighted, scaled feature vector built from expected goals, points-per-game, form, shot accuracy, dangerous attacks, league scoring context, and market-derived signals. From the predicted scoreline the system derives match outcome (1/X/2), over/under lines, both-teams-to-score, and a confidence category.

## Key Features

- **Streamlit control panel** to run each pipeline step individually or end-to-end, with live logs, progress, and file-status checks
- **Three-stage pipeline** — fetch today's matches, extract pre-match features, generate predictions
- **Ridge regression scoreline model** — separate home and away goal models with a saved feature scaler
- **Feature weighting** — domain weights applied per feature before scaling (e.g. CTMCL and market goal average weighted highest)
- **Derived markets** — match outcome (Home/Draw/Away), Over 1.5/2.5/3.5, Over/Under 2.5, and BTTS
- **Confidence scoring** — Low/Medium/High categories based on predicted goal difference
- **Incremental predictions** — only new, not-yet-predicted matches are scored, and stale predictions for dropped fixtures are cleaned up automatically
- **Results viewer** — browse live matches, extracted features, and predictions in-app with CSV download
- **Team mapping utilities** — scripts and lookup tables for reconciling team names/IDs across data sources

## How It Works

```
today_matches.py   →  live.csv                        (fetch today's fixtures)
fetch_data.py      →  extracted_features_complete.csv  (build pre-match features)
predicting.py      →  best_match_predictions.csv       (score with Ridge models)
        ▲
   app.py (Streamlit) orchestrates the above and renders results
```

The prediction stage (`predicting.py`):

1. Loads `extracted_features_complete.csv` and filters to matches not already predicted.
2. Loads `home_model.pkl`, `away_model.pkl`, and `scaler_new.pkl`.
3. Assembles the trained feature set, applies per-feature weights, and scales it.
4. Predicts home and away goals, then derives total goals, outcome, over/under lines, BTTS, and a confidence category.
5. Appends new rows to `best_match_predictions.csv` and prints a summary.

> Note: the Streamlit app invokes the prediction step as `predict.py`; in this repository the prediction logic lives in `predicting.py`. Run scripts directly (below) or align the filename to match the app before using the in-app "Run" buttons.

## Tech Stack

- **App / UI:** Streamlit
- **ML:** scikit-learn (Ridge regression), joblib for model persistence
- **Data:** pandas, numpy
- **Fetching:** requests

## Getting Started

### Prerequisites

- Python 3.x
- pip
- The trained model artifacts present in the repo root (`home_model.pkl`, `away_model.pkl`, `scaler_new.pkl`)

### Install

```bash
pip install -r requirements.txt
```

### Run the App

```bash
streamlit run app.py
```

### Run the Pipeline Manually

```bash
python today_matches.py     # → live.csv
python fetch_data.py        # → extracted_features_complete.csv
python predicting.py        # → best_match_predictions.csv
```

## Data Artifacts

| File | Role |
|------|------|
| `live.csv` | Today's fetched fixtures |
| `extracted_features_complete.csv` | Engineered pre-match features |
| `best_match_predictions.csv` | Model predictions (incrementally updated) |
| `home_model.pkl`, `away_model.pkl` | Trained Ridge goal models |
| `scaler_new.pkl` | Feature scaler used at inference |

## Project Structure

```
football-predictions/
├── app.py                  # Streamlit orchestrator + results viewer
├── today_matches.py        # Stage 1: fetch fixtures → live.csv
├── fetch_data.py           # Stage 2: build features
├── predicting.py           # Stage 3: Ridge model predictions
├── *_model.pkl, scaler_new.pkl   # Trained model artifacts
├── match_mapping.py, team_mapping_script_footy_football.py
│                           # Team name/ID reconciliation utilities
├── map*.csv                # Team mapping lookup tables
├── requirements.txt
└── LICENSE
```

The repository also contains additional experimentation and grading scripts (e.g. `ml_grade.py`, `ou_grade.py`, `v3_ml.py`, `validate_main.py`) used during model development.

## License

See the [LICENSE](LICENSE) file in this repository.
