# Football Match Prediction System

[LIVE Predictions](https://football-predictions-agility.streamlit.app)
## Advanced Machine Learning Platform for Football Analytics

A sophisticated, production-ready football match prediction system that leverages advanced machine learning algorithms and comprehensive statistical analysis to deliver accurate match outcome predictions, goal forecasts, and betting profit optimization insights.

---

## Overview

This platform represents a complete end-to-end solution for football match prediction, combining real-time data acquisition, advanced feature engineering, and Ridge Regression models to generate highly accurate predictions. The system is designed with automation at its core, featuring scheduled predictions, continuous model updates, and an intuitive web interface for seamless interaction.

### Key Capabilities

**Predictive Analytics**
- Match outcome prediction (Home Win, Draw, Away Win)
- Precise goal scoring forecasts for both teams
- Over/Under predictions across multiple thresholds (1.5, 2.5, 3.5 goals)
- Custom Total Match Closing Line (CTMCL) predictions
- Both Teams To Score (BTTS) probability analysis
- Confidence-weighted prediction categories

**Betting Intelligence**
- Moneyline profit optimization
- Over/Under 2.5 betting profit analysis
- CTMCL-based profit calculations
- High-profit opportunity identification
- Market-aligned probability assessments

**Operational Features**
- Fully automated daily prediction pipeline
- Real-time match data synchronization
- Comprehensive feature extraction (21+ statistical metrics)
- Incremental prediction updates
- Historical prediction management

---

## Technology Stack

### Core Machine Learning

**scikit-learn** (≥1.3.0)
- Ridge Regression models for home and away goal predictions
- StandardScaler for feature normalization
- Model persistence via joblib serialization

**NumPy** (≥1.24.0)
- High-performance numerical computations
- Matrix operations for feature engineering
- Statistical calculations and transformations

### Data Processing & Analytics

**Pandas** (≥2.0.0)
- Advanced DataFrame operations
- CSV data manipulation and storage
- Feature engineering pipeline
- Statistical aggregations

### Web Framework

**Streamlit** (≥1.28.0)
- Interactive web application interface
- Real-time data visualization
- Multi-tab results viewer
- Step-by-step pipeline execution controls
- Custom CSS styling for enhanced UX

### API Integration

**Requests** (≥2.31.0)
- Football-Data-API integration
- RESTful API communication
- Team and league statistics retrieval
- Rate-limited data acquisition

### Model Persistence

**joblib** (≥1.3.0)
- Efficient model serialization
- Scaler state preservation
- Quick model loading for predictions

### DevOps & Automation

**GitHub Actions**
- Scheduled daily predictions (6:00 AM UTC)
- Automated CI/CD pipeline
- Artifact backup system
- Version-controlled predictions

---

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────┐
│                    STREAMLIT WEB INTERFACE                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  Pipeline   │  │   Results   │  │  Analytics  │        │
│  │  Controls   │  │   Viewer    │  │  Dashboard  │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     PREDICTION PIPELINE                      │
│                                                               │
│  Step 1: today_matches.py                                    │
│  ├─ Fetch today's scheduled matches                          │
│  ├─ Extract match metadata                                   │
│  └─ Output: live.csv                                         │
│                                                               │
│  Step 2: fetch_data.py                                       │
│  ├─ API data acquisition (teams & leagues)                   │
│  ├─ Feature engineering (21+ metrics)                        │
│  ├─ Statistical aggregations                                 │
│  └─ Output: extracted_features_complete.csv                  │
│                                                               │
│  Step 3: predict.py                                          │
│  ├─ Load Ridge Regression models                             │
│  ├─ Feature scaling & transformation                         │
│  ├─ Generate predictions                                     │
│  ├─ Calculate betting profits                                │
│  └─ Output: best_match_predictions.csv                       │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    AUTOMATED WORKFLOWS                       │
│  ┌─────────────────────────────────────────────────┐       │
│  │  GitHub Actions - Daily Predictions (Cron)      │       │
│  │  ├─ Scheduled execution (6:00 AM UTC)           │       │
│  │  ├─ Automatic commit & push                     │       │
│  │  └─ Artifact backup (7-day retention)           │       │
│  └─────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Match Acquisition**: Real-time match data retrieved from Football-Data-API
2. **Feature Extraction**: 21+ statistical features computed per match
3. **Model Inference**: Dual Ridge Regression models predict home/away goals
4. **Post-Processing**: Outcome classification, confidence scoring, profit analysis
5. **Storage & Visualization**: Results stored in CSV and displayed via Streamlit

---

## Feature Engineering

The system employs sophisticated feature engineering to capture match dynamics comprehensively:

### Core Features

**Expected Goals (xG) Metrics**
- `team_a_xg_prematch`: Home team pre-match expected goals
- `team_b_xg_prematch`: Away team pre-match expected goals
- `home_xg_avg`: Home team's average xG (historical)
- `away_xg_avg`: Away team's average xG (historical)

**Market Intelligence**
- `CTMCL`: Custom Total Match Closing Line
- `avg_goals_market`: Market-implied total goals
- `odds_ft_1_prob`: Full-time home win probability
- `odds_ft_2_prob`: Full-time away win probability
- `o25_potential`: Over 2.5 goals market potential
- `o35_potential`: Over 3.5 goals market potential

**Team Performance**
- `pre_match_home_ppg`: Home team points per game
- `pre_match_away_ppg`: Away team points per game
- `home_form_points`: Recent form (last 5 matches)
- `away_form_points`: Recent form (last 5 matches)

**Defensive & Offensive Metrics**
- `home_goals_conceded_avg`: Average goals conceded (home)
- `away_goals_conceded_avg`: Average goals conceded (away)
- `home_shots_accuracy_avg`: Shot accuracy percentage
- `away_shots_accuracy_avg`: Shot accuracy percentage
- `home_dangerous_attacks_avg`: Dangerous attacks per match
- `away_dangerous_attacks_avg`: Dangerous attacks per match

**Contextual Data**
- `league_avg_goals`: League-specific goal scoring averages

### Feature Weighting

The system applies intelligent feature weighting to prioritize high-impact metrics:

- **CTMCL**: 2.0x weight (highest priority)
- **Market data** (avg_goals_market, odds probabilities): 1.3-1.4x weight
- **xG metrics**: 1.2-1.3x weight
- **Form & PPG**: 1.1-1.2x weight
- **Defensive & offensive stats**: 1.0-1.1x weight

---

## Machine Learning Models

### Ridge Regression Architecture

The system employs dual Ridge Regression models with optimal hyperparameters:

**Home Goals Model** (`ridge_home_model.pkl`)
- Target variable: Home team goals
- Alpha regularization: Optimized via cross-validation
- Feature space: 21+ normalized features

**Away Goals Model** (`ridge_away_model.pkl`)
- Target variable: Away team goals
- Alpha regularization: Optimized via cross-validation
- Feature space: 21+ normalized features

### Model Training Process

1. **Data Collection**: Historical match data with 21+ features
2. **Feature Scaling**: StandardScaler normalization
3. **Cross-Validation**: K-fold CV for hyperparameter tuning
4. **Model Fitting**: Ridge Regression with optimal alpha
5. **Serialization**: joblib persistence for production deployment

### Prediction Methodology

```python
# Scaled feature preparation
X_scaled = scaler.transform(X_features)

# Apply feature weights
X_weighted = X_scaled * feature_weights

# Dual model prediction
predicted_home_goals = ridge_home_model.predict(X_weighted)
predicted_away_goals = ridge_away_model.predict(X_weighted)

# Post-processing
predicted_total_goals = predicted_home_goals + predicted_away_goals
goal_difference = predicted_home_goals - predicted_away_goals

# Outcome classification
if goal_difference > threshold:
    outcome = "Home Win"
elif goal_difference < -threshold:
    outcome = "Away Win"
else:
    outcome = "Draw"
```

---

## Installation

### Prerequisites

- Python 3.11 or higher
- pip package manager
- Git

### Setup Instructions

```bash
# Clone the repository
git clone https://github.com/iampreetdave-max/football-predictions.git
cd football-predictions

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Required Files

Ensure the following files are present in the root directory:

**Machine Learning Assets**
- `ridge_home_model.pkl` - Home goals prediction model
- `ridge_away_model.pkl` - Away goals prediction model
- `scaler.pkl` - Feature scaling transformer

**Pipeline Scripts**
- `today_matches.py` - Match data retrieval
- `fetch_data.py` - Feature extraction
- `predict.py` - Prediction generation

---

## Usage

### Web Interface (Recommended)

Launch the Streamlit application for an interactive experience:

```bash
streamlit run app.py
```

The application will open in your default browser, providing:
- Manual step-by-step pipeline execution
- Real-time results visualization
- Interactive data exploration
- Downloadable CSV exports
- Comprehensive prediction analytics

### Command-Line Execution

For automated or scheduled runs:

```bash
# Complete pipeline execution
python today_matches.py    # Step 1: Fetch matches
python fetch_data.py       # Step 2: Extract features
python predict.py          # Step 3: Generate predictions
```

### Automated Predictions

The system includes GitHub Actions workflows for autonomous operation:

**Daily Predictions** (Scheduled)
- Runs automatically at 6:00 AM UTC daily
- Executes complete prediction pipeline
- Commits results to repository
- Uploads artifacts as backup

**Manual Trigger**
- Navigate to Actions tab in GitHub
- Select "Daily Football Predictions"
- Click "Run workflow"

---

## Outputs & Results

### Generated Files

**live.csv**
- Raw match data retrieved from API
- Contains match IDs, team IDs, league IDs, timestamps
- Input for feature extraction pipeline

**extracted_features_complete.csv**
- Engineered features for all matches
- 21+ statistical metrics per match
- Normalized and weighted feature vectors

**best_match_predictions.csv**
- Complete prediction dataset
- Match outcomes and goal forecasts
- Betting profit analysis
- Confidence categories (High, Medium, Low)

### Prediction Columns

**Core Predictions**
- `match_id`: Unique match identifier
- `home_team_name`: Home team designation
- `away_team_name`: Away team designation
- `predicted_home_goals`: Forecasted home team goals
- `predicted_away_goals`: Forecasted away team goals
- `predicted_total_goals`: Combined goal prediction
- `outcome_label`: Match outcome (Home Win, Draw, Away Win)

**Over/Under Markets**
- `predicted_over_1.5`: Binary flag for Over 1.5 goals
- `predicted_over_2.5`: Binary flag for Over 2.5 goals
- `predicted_over_3.5`: Binary flag for Over 3.5 goals
- `predicted_over_CTMCL`: Custom line over prediction
- `predicted_under_CTMCL`: Custom line under prediction

**Additional Insights**
- `predicted_btts`: Both Teams To Score prediction
- `confidence_category`: Prediction confidence level
- `status`: Match status (PENDING, COMPLETED, etc.)

**Betting Analytics**
- `moneyline_profit`: Projected moneyline betting profit
- `over_profit`: Over 2.5 betting profit potential
- `ctmcl_profit`: CTMCL-based betting profit

---

## Confidence Scoring

Predictions are categorized by confidence levels based on model certainty and feature quality:

**High Confidence**
- Strong goal differential prediction
- Aligned market probabilities
- High-quality feature completeness
- Historically accurate patterns

**Medium Confidence**
- Moderate goal differential
- Mixed market signals
- Adequate feature coverage
- Standard prediction scenarios

**Low Confidence**
- Narrow goal differential
- Conflicting market data
- Limited feature availability
- Edge-case matches

---

## API Integration

The system integrates with the Football-Data-API for real-time data acquisition:

**Endpoints Used**
- `/lastx` - Team historical statistics
- `/league-season` - League-specific metrics

**Rate Limiting**
- Implemented 100ms delay between requests
- Timeout handling (30 seconds per request)
- Automatic retry logic

**Data Retrieved**
- Team performance metrics (last 10 matches)
- Expected goals (xG) data
- Shot accuracy and dangerous attacks
- League averages and context

---

## GitHub Actions Workflows

### Daily Predictions Workflow

**Schedule**: 6:00 AM UTC daily (11:30 AM IST)

**Steps**:
1. Repository checkout
2. Python 3.11 environment setup
3. Dependency installation
4. Pipeline execution (3 steps)
5. Results commit and push
6. Artifact backup (7-day retention)

**Features**:
- Automatic conflict resolution
- Retry logic (5 attempts)
- Branch detection (main/master)
- Failure notifications

### Keep Streamlit Alive Workflow

Ensures the Streamlit application remains accessible and responsive.

### Data Validation Workflow

Validates data integrity and prediction quality before deployment.

---

## Project Structure

```
football-predictions/
│
├── .github/
│   └── workflows/
│       ├── daily-predictions.yml    # Automated prediction workflow
│       ├── keep-streamlit-alive.yml # Application monitoring
│       ├── save_data.yml            # Data persistence
│       └── validate.yml             # Data validation
│
├── app.py                           # Streamlit web interface
├── today_matches.py                 # Match data retrieval
├── fetch_data.py                    # Feature extraction pipeline
├── predict.py                       # Prediction generation
├── login_script.py                  # Authentication utilities
├── save.py                          # Data persistence utilities
├── validate_predictions.py          # Prediction validation
│
├── ridge_home_model.pkl             # Home goals ML model
├── ridge_away_model.pkl             # Away goals ML model
├── scaler.pkl                       # Feature scaler
│
├── requirements.txt                 # Python dependencies
├── LICENSE                          # Project license
└── README.md                        # Project documentation
```

---

## Advanced Features

### Incremental Prediction Updates

The system intelligently manages predictions to avoid redundant computations:
- Tracks previously predicted matches
- Only generates predictions for new matches
- Automatically cleans outdated predictions
- Maintains prediction history

### CTMCL-Based Predictions

Custom Total Match Closing Line (CTMCL) provides market-aligned predictions:
- Dynamic goal thresholds per match
- Market probability integration
- Betting profit optimization
- Edge detection for high-value opportunities

### Profit Optimization

Advanced betting profit calculations across multiple markets:
- Moneyline expected value
- Over/Under 2.5 profit potential
- CTMCL-specific profit analysis
- High-profit opportunity identification

---

## Performance Metrics

The system provides comprehensive performance insights:

**Goal Prediction Accuracy**
- Average predicted total goals
- Min/Max goal ranges
- CTMCL alignment

**Outcome Distribution**
- Home Win, Draw, Away Win percentages
- Confidence-weighted accuracy
- Historical validation

**Betting Profitability**
- Average profit per market
- Maximum profit opportunities
- Total potential returns
- ROI calculations

---

## Extensibility

The system is designed for easy extension and customization:

**Model Enhancement**
- Replace Ridge Regression with ensemble methods
- Incorporate neural networks
- Add classification models for discrete outcomes

**Feature Expansion**
- Player-specific statistics
- Weather conditions
- Head-to-head historical data
- Injury reports

**Market Integration**
- Real-time odds tracking
- Bookmaker-specific probabilities
- Arbitrage opportunity detection

---

## Best Practices

**For Optimal Results:**

1. **Daily Execution**: Run the pipeline daily for fresh predictions
2. **Model Retraining**: Periodically retrain models with new historical data
3. **Data Quality**: Validate API responses for completeness
4. **Feature Monitoring**: Track feature importance and drift
5. **Prediction Review**: Analyze high-confidence predictions for betting

**Data Management:**

- Archive historical predictions for backtesting
- Maintain separate environments for development and production
- Version control all model artifacts
- Document feature engineering changes

---

## Troubleshooting

### Common Issues

**Missing Model Files**
- Ensure all `.pkl` files are present in the root directory
- Verify file integrity (not corrupted during transfer)

**API Rate Limiting**
- Increase delay between requests in `fetch_data.py`
- Implement exponential backoff

**Prediction Discrepancies**
- Verify feature scaling consistency
- Check for missing values in input data
- Validate model versions

**GitHub Actions Failures**
- Review workflow logs in Actions tab
- Check API key validity
- Verify repository permissions

---

## Contributing

Contributions are welcome to enhance the system's capabilities. Areas for improvement:

- Advanced deep learning models
- Real-time prediction updates
- Enhanced betting strategies
- Multi-league support expansion
- Performance optimization
- Documentation enhancements

---

## License

This project is licensed under the MIT License. See the LICENSE file for details.

---

## Disclaimer

This system is designed for educational and analytical purposes. Predictions are based on statistical models and historical data. Users should conduct their own research and exercise caution when making betting decisions. The authors assume no liability for financial decisions based on these predictions.

---

## Acknowledgments

- Football-Data-API for comprehensive football statistics
- scikit-learn development team for robust ML tools
- Streamlit for intuitive web application framework
- GitHub Actions for seamless automation

---

## Contact

**Repository**: [https://github.com/iampreetdave-max/football-predictions](https://github.com/iampreetdave-max/football-predictions)

**Author**: Preet Dave

For questions, suggestions, or collaboration opportunities, please open an issue in the GitHub repository.

---

**Built with precision. Powered by data. Driven by accuracy.**
