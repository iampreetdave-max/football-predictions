"""
Production O/U Grade Update - Complete Implementation
======================================================

Updates predictions_soccer_v1_ourmodel with ou_grade (A/B/C/D) and ou_confidence.

Grades:
- A: 88.9% WR, +0.1159 avg profit ✅
- B: 76.9% WR, +0.0385 avg profit ✅
- C: 51.2% WR, -0.12 avg profit
- D: 62.4% WR, -0.05 avg profit
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import numpy as np
import logging
from datetime import datetime
import os
# ============================================================================
# CONFIGURATION
# ============================================================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

TABLE_NAME = "predictions_soccer_v1_ourmodel"

# Thresholds (optimized for max A+B profitability)
OU_A_THRESHOLD = 0.750000
OU_B_THRESHOLD = 0.700000
OU_C_THRESHOLD = 0.266667  # Old Q3 baseline

BATCH_SIZE = 100

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"ou_grade_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# FUNCTIONS
# ============================================================================

def get_db_connection():
    """Connect to database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("[OK] Connected to database")
        return conn
    except Exception as e:
        logger.error(f"[ERROR] Connection failed: {e}")
        raise


def fetch_null_ou_grade_records(conn) -> pd.DataFrame:
    """Fetch records where ou_grade IS NULL."""
    query = f"""
    SELECT 
        match_id,
        predicted_home_goals,
        predicted_away_goals,
        predicted_outcome,
        over_2_5_odds,
        under_2_5_odds
    FROM {TABLE_NAME}
    WHERE ou_grade IS NULL
    ORDER BY match_id
    """
    
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"[OK] Fetched {len(df)} records with NULL ou_grade")
        return df
    except Exception as e:
        logger.error(f"[ERROR] Fetch failed: {e}")
        raise


def calculate_advanced_ou_confidence(row) -> float:
    """Calculate advanced O/U confidence metric."""
    try:
        total_predicted = row['predicted_home_goals'] + row['predicted_away_goals']
        distance_from_threshold = abs(total_predicted - 2.5)
        
        over_odds = row['over_2_5_odds']
        under_odds = row['under_2_5_odds']
        predicted_ou = row['predicted_outcome']
        
        if pd.isna(distance_from_threshold) or pd.isna(over_odds) or pd.isna(under_odds):
            return np.nan
        if over_odds <= 0 or under_odds <= 0:
            return np.nan
        
        # Market favorite
        if over_odds < under_odds:
            market_favorite = "over"
            min_odds = over_odds
            max_odds = under_odds
        else:
            market_favorite = "under"
            min_odds = under_odds
            max_odds = over_odds
        
        # Prediction direction
        if "Over" in str(predicted_ou):
            pred_direction = "over"
            pred_odds = over_odds
        elif "Under" in str(predicted_ou):
            pred_direction = "under"
            pred_odds = under_odds
        else:
            return np.nan
        
        # Market factor
        if pred_direction == market_favorite:
            market_factor = 1.0 / pred_odds
        else:
            market_factor = pred_odds / min_odds
        
        # Asymmetric distance adjustment
        if pred_direction == "over":
            if total_predicted >= 2.5:
                distance_factor = distance_from_threshold
            else:
                distance_factor = distance_from_threshold * (min_odds / max_odds)
        else:
            if total_predicted <= 2.5:
                distance_factor = distance_from_threshold
            else:
                distance_factor = distance_from_threshold * (min_odds / max_odds)
        
        confidence = distance_factor * market_factor
        return confidence if confidence >= 0 else np.nan
        
    except Exception as e:
        logger.warning(f"Confidence calc error: {e}")
        return np.nan


def assign_ou_grade(confidence) -> str:
    """Assign grade based on confidence."""
    if pd.isna(confidence):
        return "D"
    
    if confidence >= OU_A_THRESHOLD:
        return "A"
    elif confidence >= OU_B_THRESHOLD:
        return "B"
    elif confidence >= OU_C_THRESHOLD:
        return "C"
    else:
        return "D"


def calculate_ou_grades(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate grades for all records."""
    logger.info(f"\nCalculating O/U grades for {len(df)} records...")
    
    # Calculate confidence
    df['ou_confidence'] = df.apply(calculate_advanced_ou_confidence, axis=1)
    
    # Assign grades
    df['ou_grade'] = df['ou_confidence'].apply(assign_ou_grade)
    
    # Convert to percentage
    df['ou_confidence_pct'] = (df['ou_confidence'] * 100).clip(upper=100).fillna(0)
    
    logger.info("[OK] Grades calculated")
    logger.info(f"  A: {(df['ou_grade'] == 'A').sum()} | B: {(df['ou_grade'] == 'B').sum()} | " +
               f"C: {(df['ou_grade'] == 'C').sum()} | D: {(df['ou_grade'] == 'D').sum()}")
    
    return df[["match_id", "ou_grade", "ou_confidence_pct"]]


def update_database(conn, grades_df: pd.DataFrame) -> int:
    """Update database with grades."""
    if len(grades_df) == 0:
        logger.warning("No records to update")
        return 0
    
    logger.info(f"\nUpdating {len(grades_df)} records...")
    
    cursor = conn.cursor()
    
    update_query = f"""
    UPDATE {TABLE_NAME}
    SET 
        ou_grade = %s,
        ou_confidence = %s,
        updated_at = NOW()
    WHERE match_id = %s
    """
    
    update_data = [
        (row["ou_grade"], float(row["ou_confidence_pct"]), float(row["match_id"]))
        for _, row in grades_df.iterrows()
    ]
    
    try:
        execute_batch(cursor, update_query, update_data, page_size=BATCH_SIZE)
        conn.commit()
        logger.info(f"[OK] Updated {len(grades_df)} records")
        return len(grades_df)
    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] Update failed: {e}")
        raise
    finally:
        cursor.close()


def print_summary(grades_df: pd.DataFrame):
    """Print summary."""
    logger.info("\n" + "=" * 80)
    logger.info("SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Records: {len(grades_df)}")
    logger.info(f"\nGrades:")
    logger.info(f"  A (88.9% WR, +0.1159): {(grades_df['ou_grade'] == 'A').sum()}")
    logger.info(f"  B (76.9% WR, +0.0385): {(grades_df['ou_grade'] == 'B').sum()}")
    logger.info(f"  C (51.2% WR, -0.12):   {(grades_df['ou_grade'] == 'C').sum()}")
    logger.info(f"  D (62.4% WR, -0.05):   {(grades_df['ou_grade'] == 'D').sum()}")
    logger.info("=" * 80 + "\n")


def main():
    """Main execution."""
    logger.info("=" * 80)
    logger.info("O/U GRADE UPDATE - A/B/C/D IMPLEMENTATION")
    logger.info("=" * 80)
    
    conn = None
    try:
        conn = get_db_connection()
        df_records = fetch_null_ou_grade_records(conn)
        
        if len(df_records) == 0:
            logger.info("No records to process")
            return
        
        df_grades = calculate_ou_grades(df_records)
        updated_count = update_database(conn, df_grades)
        print_summary(df_grades)
        
        logger.info("[OK] Process completed successfully")
        
    except Exception as e:
        logger.error(f"[ERROR] Process failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Connection closed")


if __name__ == "__main__":
    main()
