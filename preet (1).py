"""
ULTIMATE FOOTBALL PREDICTOR - FINAL VERSION
- 100 epochs for feature selection
- Saves best features to JSON
- Proper confidence distribution (HIGH=few, MEDIUM=middle, LOW=most)
- Better O/U 2.5 accuracy
- All pre-match features only
- FIXED: odds_under25 extraction and ROI calculations
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor, GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import mean_absolute_error, accuracy_score
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor, XGBClassifier
from datetime import datetime
import warnings
import joblib
import os
import json
warnings.filterwarnings('ignore')

DATASET_SIZE = 50000
RANDOM_SEED = 42
GPU_PARAMS = {'device': 'cuda', 'tree_method': 'hist'}

class DataLoader:
    def load(self, filepath):
        print("\n" + "="*80)
        print("LOADING DATA")
        print("="*80)
        
        df = pd.read_csv(filepath).sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)
        if len(df) > DATASET_SIZE:
            df = df.head(DATASET_SIZE)
        
        df['date'] = pd.to_datetime(df['date_unix'], unit='s', errors='coerce')
        df = df.dropna(subset=['date'])
        
        df['home_team'] = df['home_name'].str.strip()
        df['away_team'] = df['away_name'].str.strip()
        df['league'] = df.get('fetched_league_name', 'Unknown').fillna('Unknown')
        
        # Target variables
        df['home_goals'] = pd.to_numeric(df['homeGoalCount'], errors='coerce')
        df['away_goals'] = pd.to_numeric(df['awayGoalCount'], errors='coerce')
        df = df.dropna(subset=['home_goals', 'away_goals'])
        df['total_goals'] = df['home_goals'] + df['away_goals']
        
        # PRE-MATCH xG only
        df['pre_home_xg'] = pd.to_numeric(df['team_a_xg_prematch'], errors='coerce')
        df['pre_away_xg'] = pd.to_numeric(df['team_b_xg_prematch'], errors='coerce')
        df = df.dropna(subset=['pre_home_xg', 'pre_away_xg'])
        df = df[(df['pre_home_xg'] > 0) & (df['pre_away_xg'] > 0)]
        
        df['pre_total_xg'] = df['pre_home_xg'] + df['pre_away_xg']
        df['xg_diff'] = df['pre_home_xg'] - df['pre_away_xg']
        df['xg_ratio'] = df['pre_home_xg'] / (df['pre_away_xg'] + 0.01)
        
        # PRE-MATCH PPG only
        df['home_ppg'] = pd.to_numeric(df['pre_match_home_ppg'], errors='coerce')
        df['away_ppg'] = pd.to_numeric(df['pre_match_away_ppg'], errors='coerce')
        df = df.dropna(subset=['home_ppg', 'away_ppg'])
        df['ppg_diff'] = df['home_ppg'] - df['away_ppg']
        
        # CTMCL for O/U 2.5 - WITH odds_under25
        if 'odds_ft_over25' in df.columns:
            df['odds_over25'] = pd.to_numeric(df['odds_ft_over25'], errors='coerce')
            df = df.dropna(subset=['odds_over25'])
            df['CTMCL'] = 2.5 + (1 / df['odds_over25'] - 0.5)
            df = df[(df['CTMCL'] > 0) & (df['CTMCL'] < 10)]
        else:
            df['CTMCL'] = 2.5
        
        # Extract odds_under25 if available
        if 'odds_ft_under25' in df.columns:
            df['odds_under25'] = pd.to_numeric(df['odds_ft_under25'], errors='coerce')
        else:
            # Calculate implied under odds from over odds using margin
            if 'odds_over25' in df.columns:
                # Standard bookmaker margin: under_odds ≈ 1 / (1 - 1/over_odds)
                df['odds_under25'] = 1 / (1 - 1 / (df['odds_over25'] + 0.01))
            else:
                df['odds_under25'] = 2.0
        
        # Fill any remaining NaN odds_under25 with default
        df['odds_under25'] = df['odds_under25'].fillna(2.0)
        
        # Market potentials (pre-match)
        for col in ['o25_potential', 'o35_potential', 'o45_potential', 'btts_potential']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(50)
            else:
                df[col] = 50
        
        # Odds
        for old, new in [('odds_ft_1', 'odds_home_win'), ('odds_ft_2', 'odds_away_win')]:
            if old in df.columns:
                df[new] = pd.to_numeric(df[old], errors='coerce').fillna(2.5)
        
        df = df.sort_values('date').reset_index(drop=True)
        print(f"✓ Loaded {len(df)} matches (all PRE-MATCH features)")
        print(f"✓ Over 2.5 odds range: {df['odds_over25'].min():.2f} - {df['odds_over25'].max():.2f}")
        print(f"✓ Under 2.5 odds range: {df['odds_under25'].min():.2f} - {df['odds_under25'].max():.2f}")
        return df


class FeatureEngine:
    def create(self, df):
        print("\n" + "="*80)
        print("CREATING HISTORICAL FEATURES")
        print("="*80)
        
        features = [
            'home_xg_avg', 'away_xg_avg', 'home_xg_recent', 'away_xg_recent',
            'home_ppg_avg', 'away_ppg_avg', 'home_elo', 'away_elo', 'elo_diff',
            'h2h_total_goals', 'home_form', 'away_form', 'league_avg_goals',
            'home_goals_avg', 'away_goals_avg'
        ]
        
        for col in features:
            df[col] = np.nan
        
        team_elo = {}
        
        for i in range(len(df)):
            if i % 500 == 0:
                print(f"  [{i}/{len(df)}]")
            
            home = df.iloc[i]['home_team']
            away = df.iloc[i]['away_team']
            past = df.iloc[:i]
            
            if home not in team_elo:
                team_elo[home] = 1500
            if away not in team_elo:
                team_elo[away] = 1500
            
            df.at[i, 'home_elo'] = team_elo[home]
            df.at[i, 'away_elo'] = team_elo[away]
            df.at[i, 'elo_diff'] = team_elo[home] - team_elo[away]
            
            if len(past) > 0:
                # Home history
                home_past = past[(past['home_team'] == home) | (past['away_team'] == home)]
                if len(home_past) >= 5:
                    home_xg, home_ppg, home_pts, home_goals = [], [], [], []
                    for _, m in home_past.iterrows():
                        if m['home_team'] == home:
                            home_xg.append(m['pre_home_xg'])
                            home_ppg.append(m['home_ppg'])
                            home_goals.append(m['home_goals'])
                            home_pts.append(3 if m['home_goals'] > m['away_goals'] else (1 if m['home_goals'] == m['away_goals'] else 0))
                        else:
                            home_xg.append(m['pre_away_xg'])
                            home_ppg.append(m['away_ppg'])
                            home_goals.append(m['away_goals'])
                            home_pts.append(3 if m['away_goals'] > m['home_goals'] else (1 if m['away_goals'] == m['home_goals'] else 0))
                    
                    df.at[i, 'home_xg_avg'] = np.mean(home_xg)
                    df.at[i, 'home_xg_recent'] = np.mean(home_xg[-5:])
                    df.at[i, 'home_ppg_avg'] = np.mean(home_ppg)
                    df.at[i, 'home_form'] = sum(home_pts[-5:])
                    df.at[i, 'home_goals_avg'] = np.mean(home_goals)
                
                # Away history
                away_past = past[(past['home_team'] == away) | (past['away_team'] == away)]
                if len(away_past) >= 5:
                    away_xg, away_ppg, away_pts, away_goals = [], [], [], []
                    for _, m in away_past.iterrows():
                        if m['home_team'] == away:
                            away_xg.append(m['pre_home_xg'])
                            away_ppg.append(m['home_ppg'])
                            away_goals.append(m['home_goals'])
                            away_pts.append(3 if m['home_goals'] > m['away_goals'] else (1 if m['home_goals'] == m['away_goals'] else 0))
                        else:
                            away_xg.append(m['pre_away_xg'])
                            away_ppg.append(m['away_ppg'])
                            away_goals.append(m['away_goals'])
                            away_pts.append(3 if m['away_goals'] > m['home_goals'] else (1 if m['away_goals'] == m['home_goals'] else 0))
                    
                    df.at[i, 'away_xg_avg'] = np.mean(away_xg)
                    df.at[i, 'away_xg_recent'] = np.mean(away_xg[-5:])
                    df.at[i, 'away_ppg_avg'] = np.mean(away_ppg)
                    df.at[i, 'away_form'] = sum(away_pts[-5:])
                    df.at[i, 'away_goals_avg'] = np.mean(away_goals)
                
                # H2H
                h2h = past[((past['home_team'] == home) & (past['away_team'] == away)) |
                          ((past['home_team'] == away) & (past['away_team'] == home))]
                df.at[i, 'h2h_total_goals'] = h2h['total_goals'].mean() if len(h2h) > 0 else 2.5
                
                # League
                league_past = past[past['league'] == df.iloc[i]['league']]
                df.at[i, 'league_avg_goals'] = league_past['total_goals'].mean() if len(league_past) > 0 else 2.5
            
            # Update Elo
            result = 1.0 if df.iloc[i]['home_goals'] > df.iloc[i]['away_goals'] else (0.0 if df.iloc[i]['home_goals'] < df.iloc[i]['away_goals'] else 0.5)
            expected = 1 / (1 + 10 ** ((team_elo[away] - team_elo[home]) / 400))
            team_elo[home] += 20 * (result - expected)
            team_elo[away] += 20 * ((1 - result) - (1 - expected))
        
        df = df.dropna(subset=features)
        df = df.iloc[30:].reset_index(drop=True)
        print(f"✓ Features created: {len(df)} matches")
        return df


class FeatureSelector:
    def select_best_features(self, df_train, df_val, candidates, target, model_type='regressor', epochs=100):
        print(f"\n→ Feature selection: {len(candidates)} candidates, {epochs} epochs...")
        
        best_features = candidates[:12]
        best_score = -float('inf')
        
        for epoch in range(epochs):
            n = np.random.randint(12, min(len(candidates)+1, 22))
            features = list(np.random.choice(candidates, n, replace=False))
            
            try:
                X_train = df_train[features]
                X_val = df_val[features]
                y_train = df_train[target] if target in df_train.columns else target
                y_val = df_val[target] if target in df_val.columns else target
                
                if model_type == 'regressor':
                    model = XGBRegressor(n_estimators=150, max_depth=5, learning_rate=0.05, random_state=RANDOM_SEED, **GPU_PARAMS)
                    model.fit(X_train, y_train, verbose=False)
                    pred = model.predict(X_val)
                    score = -mean_absolute_error(y_val, pred)
                else:
                    model = XGBClassifier(n_estimators=150, max_depth=5, learning_rate=0.05, random_state=RANDOM_SEED, **GPU_PARAMS)
                    model.fit(X_train, y_train, verbose=False)
                    pred = model.predict(X_val)
                    score = accuracy_score(y_val, pred)
                
                if score > best_score:
                    best_score = score
                    best_features = features
                    if epoch % 20 == 0 or score > best_score:
                        metric = "MAE" if model_type == 'regressor' else "Acc"
                        print(f"  Epoch {epoch+1}: {len(features)} features, {metric}={abs(best_score):.4f}")
            except:
                continue
        
        print(f"✓ Best: {len(best_features)} features")
        return best_features


class ModelTrainer:
    def __init__(self):
        self.scalers = {}
        self.models = {}
        self.selector = FeatureSelector()
        self.best_features = {}
    
    def train(self, df_train, df_val, df_test):
        print("\n" + "="*80)
        print("TRAINING MODELS (100 EPOCHS)")
        print("="*80)
        
        os.makedirs('models', exist_ok=True)
        
        # === O/U 2.5 MODEL (PRIORITY) ===
        print("\n→ O/U 2.5 Model")
        ou_candidates = [
            'pre_total_xg', 'CTMCL', 'pre_home_xg', 'pre_away_xg', 
            'o25_potential', 'o35_potential', 'o45_potential', 'btts_potential',
            'home_xg_avg', 'away_xg_avg', 'home_xg_recent', 'away_xg_recent',
            'home_goals_avg', 'away_goals_avg', 'league_avg_goals', 'h2h_total_goals',
            'home_form', 'away_form', 'home_ppg_avg', 'away_ppg_avg',
            'home_elo', 'away_elo', 'elo_diff', 'xg_diff', 'xg_ratio', 'ppg_diff'
        ]
        ou_candidates = [f for f in ou_candidates if f in df_train.columns]
        
        y_train_ou = (df_train['total_goals'] > 2.5).astype(int)
        y_val_ou = (df_val['total_goals'] > 2.5).astype(int)
        y_test_ou = (df_test['total_goals'] > 2.5).astype(int)
        
        ou_features = self.selector.select_best_features(df_train, df_val, ou_candidates, y_train_ou, 'classifier', epochs=100)
        self.best_features['ou'] = ou_features
        
        scaler_ou = StandardScaler()
        X_train_ou = scaler_ou.fit_transform(df_train[ou_features])
        X_val_ou = scaler_ou.transform(df_val[ou_features])
        X_test_ou = scaler_ou.transform(df_test[ou_features])
        
        # Ensemble
        xgb_ou = XGBClassifier(n_estimators=300, max_depth=7, learning_rate=0.02, subsample=0.8, 
                               colsample_bytree=0.8, random_state=RANDOM_SEED, early_stopping_rounds=25, **GPU_PARAMS)
        xgb_ou.fit(X_train_ou, y_train_ou, eval_set=[(X_val_ou, y_val_ou)], verbose=False)
        
        gb_ou = GradientBoostingClassifier(n_estimators=300, max_depth=7, learning_rate=0.02, subsample=0.8, random_state=RANDOM_SEED)
        gb_ou.fit(X_train_ou, y_train_ou)
        
        rf_ou = RandomForestClassifier(n_estimators=250, max_depth=15, min_samples_leaf=2, random_state=RANDOM_SEED)
        rf_ou.fit(X_train_ou, y_train_ou)
        
        # Predictions
        pred_ou_xgb = xgb_ou.predict_proba(X_test_ou)[:, 1]
        pred_ou_gb = gb_ou.predict_proba(X_test_ou)[:, 1]
        pred_ou_rf = rf_ou.predict_proba(X_test_ou)[:, 1]
        
        pred_ou_proba = (pred_ou_xgb * 0.45 + pred_ou_gb * 0.35 + pred_ou_rf * 0.20)
        pred_ou = (pred_ou_proba > 0.5).astype(int)
        
        ou_acc = (pred_ou == y_test_ou).mean()
        print(f"  O/U 2.5 Accuracy: {ou_acc:.1%}")
        
        self.models['ou'] = {'xgb': xgb_ou, 'gb': gb_ou, 'rf': rf_ou}
        self.scalers['ou'] = scaler_ou
        
        # === MONEYLINE MODEL ===
        print("\n→ Moneyline Model")
        ml_candidates = [
            'elo_diff', 'xg_diff', 'ppg_diff', 'home_form', 'away_form',
            'home_elo', 'away_elo', 'home_xg_avg', 'away_xg_avg',
            'home_xg_recent', 'away_xg_recent', 'pre_home_xg', 'pre_away_xg',
            'home_ppg_avg', 'away_ppg_avg', 'xg_ratio', 'h2h_total_goals',
            'league_avg_goals', 'home_goals_avg', 'away_goals_avg'
        ]
        ml_candidates = [f for f in ml_candidates if f in df_train.columns]
        
        y_train_ml = (df_train['home_goals'] > df_train['away_goals']).astype(int)
        y_val_ml = (df_val['home_goals'] > df_val['away_goals']).astype(int)
        y_test_ml = (df_test['home_goals'] > df_test['away_goals']).astype(int)
        
        ml_features = self.selector.select_best_features(df_train, df_val, ml_candidates, y_train_ml, 'classifier', epochs=100)
        self.best_features['ml'] = ml_features
        
        scaler_ml = StandardScaler()
        X_train_ml = scaler_ml.fit_transform(df_train[ml_features])
        X_val_ml = scaler_ml.transform(df_val[ml_features])
        X_test_ml = scaler_ml.transform(df_test[ml_features])
        
        xgb_ml = XGBClassifier(n_estimators=300, max_depth=7, learning_rate=0.02, subsample=0.8,
                               colsample_bytree=0.8, random_state=RANDOM_SEED, early_stopping_rounds=25, **GPU_PARAMS)
        xgb_ml.fit(X_train_ml, y_train_ml, eval_set=[(X_val_ml, y_val_ml)], verbose=False)
        
        gb_ml = GradientBoostingClassifier(n_estimators=300, max_depth=7, learning_rate=0.02, subsample=0.8, random_state=RANDOM_SEED)
        gb_ml.fit(X_train_ml, y_train_ml)
        
        pred_ml_proba = xgb_ml.predict_proba(X_test_ml)[:, 1] * 0.6 + gb_ml.predict_proba(X_test_ml)[:, 1] * 0.4
        pred_ml = (pred_ml_proba > 0.5).astype(int)
        ml_acc = (pred_ml == y_test_ml).mean()
        print(f"  ML Accuracy: {ml_acc:.1%}")
        
        self.models['ml'] = {'xgb': xgb_ml, 'gb': gb_ml}
        self.scalers['ml'] = scaler_ml
        
        # Save models and features
        joblib.dump(self.models, 'models/all_models.pkl')
        joblib.dump(self.scalers, 'models/all_scalers.pkl')
        
        with open('models/best_features.json', 'w') as f:
            json.dump(self.best_features, f, indent=2)
        
        print("\n✓ Models and features saved")
        
        return {
            'pred_ou': pred_ou,
            'pred_ou_proba': pred_ou_proba,
            'actual_ou': y_test_ou,
            'pred_ml': pred_ml,
            'pred_ml_proba': pred_ml_proba,
            'actual_ml': y_test_ml,
            'ou_acc': ou_acc,
            'ml_acc': ml_acc
        }


class OutputGenerator:
    def generate(self, df_test, predictions):
        print("\n" + "="*80)
        print("GENERATING OUTPUT")
        print("="*80)
        
        output = df_test[['date', 'league', 'home_team', 'away_team', 'home_goals', 'away_goals', 'total_goals']].copy()
        
        output['pred_ou'] = predictions['pred_ou']
        output['pred_ou_proba'] = predictions['pred_ou_proba']
        output['actual_ou'] = predictions['actual_ou']
        output['ou_correct'] = (predictions['pred_ou'] == predictions['actual_ou']).astype(int)
        
        output['pred_ml'] = predictions['pred_ml']
        output['pred_ml_proba'] = predictions['pred_ml_proba']
        output['actual_ml'] = predictions['actual_ml']
        output['ml_correct'] = (predictions['pred_ml'] == predictions['actual_ml']).astype(int)
        
        # PROPER CONFIDENCE: Based on probability distance from 0.5
        # Higher confidence = further from 0.5 (more certain)
        ou_certainty = np.abs(predictions['pred_ou_proba'] - 0.5) * 200  # 0-100
        ml_certainty = np.abs(predictions['pred_ml_proba'] - 0.5) * 200  # 0-100
        
        output['ou_confidence'] = ou_certainty
        output['ml_confidence'] = ml_certainty
        output['overall_confidence'] = (ou_certainty * 0.5 + ml_certainty * 0.5)
        
        # Odds - NOW WITH odds_under25
        for col in ['odds_over25', 'odds_under25', 'odds_home_win', 'odds_away_win']:
            output[col] = df_test[col] if col in df_test.columns else 2.0
        
        # PROPER SIGNALS: HIGH should be FEW
        # Use percentiles: Top 10% = HIGH, Next 20% = MEDIUM, Rest = LOW
        high_threshold = np.percentile(output['overall_confidence'], 90)  # Top 10%
        medium_threshold = np.percentile(output['overall_confidence'], 70)  # Top 30%
        
        output['signal'] = 'LOW'
        output.loc[output['overall_confidence'] >= medium_threshold, 'signal'] = 'MEDIUM'
        output.loc[output['overall_confidence'] >= high_threshold, 'signal'] = 'HIGH'
        
        # Save
        filename = f'predictions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        output.to_csv(filename, index=False, float_format='%.4f')
        print(f"✓ Saved: {filename}")
        
        # ROI Analysis - FIXED with correct odds_under25 handling
        self._calculate_roi(output)
        
        return output
    
    def _calculate_roi(self, df):
        print("\n" + "="*80)
        print("ROI ANALYSIS")
        print("="*80)
        
        for level in ['HIGH', 'MEDIUM', 'LOW']:
            level_df = df[df['signal'] == level]
            if len(level_df) == 0:
                continue
            
            # O/U 2.5 - FIXED: Now properly separates Over vs Under
            ou_profit = 0
            ou_wins = 0
            for _, row in level_df.iterrows():
                if row['pred_ou'] == 1:  # We predict Over
                    if row['actual_ou'] == 1:  # Over hit
                        ou_profit += (row['odds_over25'] - 1)
                        ou_wins += 1
                    else:  # Over lost
                        ou_profit -= 1
                else:  # We predict Under
                    if row['actual_ou'] == 0:  # Under hit
                        ou_profit += (row['odds_under25'] - 1)
                        ou_wins += 1
                    else:  # Under lost
                        ou_profit -= 1
            
            ou_roi = (ou_profit / len(level_df)) * 100
            ou_acc = ou_wins / len(level_df) * 100
            
            # ML
            ml_profit = 0
            ml_wins = 0
            for _, row in level_df.iterrows():
                if row['pred_ml'] == 1:
                    if row['actual_ml'] == 1:
                        ml_profit += (row['odds_home_win'] - 1)
                        ml_wins += 1
                    else:
                        ml_profit -= 1
                else:
                    if row['actual_ml'] == 0:
                        ml_profit += (row['odds_away_win'] - 1)
                        ml_wins += 1
                    else:
                        ml_profit -= 1
            ml_roi = (ml_profit / len(level_df)) * 100
            ml_acc = ml_wins / len(level_df) * 100
            
            total_profit = ou_profit + ml_profit
            total_roi = (total_profit / (len(level_df) * 2)) * 100
            
            print(f"\n{level} ({len(level_df)} matches = {len(level_df)*2} bets):")
            print(f"  O/U 2.5: {ou_acc:.1f}% | ROI: {ou_roi:.2f}% | Profit: {ou_profit:.2f}u")
            print(f"  ML:      {ml_acc:.1f}% | ROI: {ml_roi:.2f}% | Profit: {ml_profit:.2f}u")
            print(f"  TOTAL:   ROI: {total_roi:.2f}% | Profit: {total_profit:.2f}u")


class Predictor:
    def run(self, filepath):
        print("\n" + "="*80)
        print("🚀 ULTIMATE FOOTBALL PREDICTOR - FINAL")
        print("="*80)
        
        loader = DataLoader()
        df = loader.load(filepath)
        
        engine = FeatureEngine()
        df = engine.create(df)
        
        train_size = int(len(df) * 0.6)
        val_size = int(len(df) * 0.2)
        df_train = df.iloc[:train_size]
        df_val = df.iloc[train_size:train_size+val_size]
        df_test = df.iloc[train_size+val_size:]
        print(f"\n✓ Split: Train={len(df_train)}, Val={len(df_val)}, Test={len(df_test)}")
        
        trainer = ModelTrainer()
        predictions = trainer.train(df_train, df_val, df_test)
        
        generator = OutputGenerator()
        output = generator.generate(df_test, predictions)
        
        print("\n" + "="*80)
        print("✅ COMPLETE")
        print("="*80)
        print(f"O/U 2.5 Accuracy: {predictions['ou_acc']:.1%}")
        print(f"ML Accuracy: {predictions['ml_acc']:.1%}")
        print(f"\nDistribution:")
        print(f"  HIGH:   {(output['signal'] == 'HIGH').sum()} matches (~10%)")
        print(f"  MEDIUM: {(output['signal'] == 'MEDIUM').sum()} matches (~20%)")
        print(f"  LOW:    {(output['signal'] == 'LOW').sum()} matches (~70%)")
        
        return output, predictions


if __name__ == "__main__":
    predictor = Predictor()
    output, predictions = predictor.run('top.csv')