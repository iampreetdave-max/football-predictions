"""
Update Database Grades Script - DUAL TABLES
============================================

Connects to PostgreSQL database and updates ml_grade and ml_confidence columns
using market-aware confidence grading.

Updates BOTH tables:
- predictions_soccer_v1_ourmodel
- predictions_soccer_v3_ourmodel

Only updates records where ml_grade is NULL AND home_odds is NOT NULL.

Database: winbets-predictions.postgres.database.azure.com
Columns updated: ml_grade, ml_confidence
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import numpy as np
from typing import Tuple, List, Dict
import logging
from datetime import datetime
import os

# ============================================================================
# CONFIGURATION
# ============================================================================

# Database credentials
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_DATABASE'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

TABLE_NAMES = ["predictions_soccer_v1_ourmodel", "predictions_soccer_v3_ourmodel"]

# Hardcoded quantile thresholds from historical v1shift data analysis
Q1_THRESHOLD = 0.1264  # 12.64%
Q2_THRESHOLD = 0.3064  # 30.64%
Q3_THRESHOLD = 0.6194  # 61.94%

# Batch size for updates
BATCH_SIZE = 100

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"grade_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# FUNCTIONS
# ============================================================================

def get_db_connection():
    """Establish connection to PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("[OK] Connected to PostgreSQL database")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"[ERROR] Failed to connect to database: {e}")
        raise


def fetch_null_grade_records(conn, table_name) -> pd.DataFrame:
    """Fetch records where ml_grade is NULL and home_odds is NOT NULL from specified table."""
    query = f"""
    SELECT 
        match_id,
        predicted_home_goals,
        predicted_away_goals,
        predicted_winner,
        home_odds,
        away_odds,
        draw_odds
    FROM {table_name}
    WHERE ml_grade IS NULL
      AND home_odds IS NOT NULL
    ORDER BY match_id
    """
    
    try:
        df = pd.read_sql(query, conn)
        logger.info(f"[OK] Fetched {len(df)} records with NULL ml_grade and non-NULL home_odds from {table_name}")
        return df
    except Exception as e:
        logger.error(f"[ERROR] Error fetching records from {table_name}: {e}")
        raise


def get_pred_side(row) -> str:
    """Extract predicted side from predicted_winner."""
    if row["predicted_winner"] == "Home Win":
        return "home"
    elif row["predicted_winner"] == "Away Win":
        return "away"
    elif row["predicted_winner"] == "Draw":
        return "draw"
    else:
        return np.nan


def get_pred_side_odds(row) -> float:
    """Get the odds for the predicted side."""
    if row["pred_side"] == "home":
        return row["home_odds"]
    elif row["pred_side"] == "away":
        return row["away_odds"]
    elif row["pred_side"] == "draw":
        return row["draw_odds"]
    return np.nan


def calc_market_factor(row) -> float:
    """
    Calculate market alignment factor.
    
    - If predicting market favourite (lowest odds): factor = 1/odds (penalize)
    - If predicting underdog/value: factor = underdog_odds/favourite_odds (amplify)
    """
    po = row["pred_side_odds"]
    min_odds = row["min_all_odds"]
    
    if pd.isna(po) or pd.isna(min_odds) or po <= 0 or min_odds <= 0:
        return np.nan
    
    # Predicting market favourite
    if np.isclose(po, min_odds, rtol=1e-5):
        return 1.0 / po
    # Predicting underdog/value
    else:
        return po / min_odds


def grade_by_confidence_inverted(confidence: float) -> str:
    """
    Assign grade based on cal_confidence using inverted thresholds.
    
    Lower confidence = Better historical ROI
    """
    if pd.isna(confidence):
        return None
    
    if confidence < Q1_THRESHOLD:
        return "A"
    elif confidence < Q2_THRESHOLD:
        return "B"
    elif confidence < Q3_THRESHOLD:
        return "C"
    else:
        return "D"


def calculate_grades(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate ml_grade and ml_confidence for dataframe."""
    logger.info(f"\nCalculating grades for {len(df)} records...")
    
    # Step 1: Calculate goal difference
    df["pred_goal_diff"] = df["predicted_home_goals"] - df["predicted_away_goals"]
    df["abs_pred_goal_diff"] = df["pred_goal_diff"].abs()
    
    # Step 2: Extract predicted side
    df["pred_side"] = df.apply(get_pred_side, axis=1)
    
    # Step 3: Get predicted side odds
    df["pred_side_odds"] = df.apply(get_pred_side_odds, axis=1)
    
    # Step 4: Get market favourite odds
    df["min_all_odds"] = df[["home_odds", "away_odds", "draw_odds"]].min(axis=1)
    
    # Step 5: Calculate market factor
    df["market_factor"] = df.apply(calc_market_factor, axis=1)
    
    # Step 6: Calculate confidence (decimal scale)
    df["cal_confidence"] = df["abs_pred_goal_diff"] * df["market_factor"]
    
    # Step 7: Convert to percentage and cap at 100%
    df["ml_confidence"] = (df["cal_confidence"] * 100).clip(upper=100)
    
    # Step 8: Assign grade
    df["ml_grade"] = df["cal_confidence"].apply(grade_by_confidence_inverted)
    
    # Handle any NULL grades (shouldn't happen, but for safety)
    df["ml_grade"] = df["ml_grade"].fillna("D")
    
    logger.info("[OK] Grades calculated")
    logger.info(f"  Grade A: {(df['ml_grade'] == 'A').sum()} records")
    logger.info(f"  Grade B: {(df['ml_grade'] == 'B').sum()} records")
    logger.info(f"  Grade C: {(df['ml_grade'] == 'C').sum()} records")
    logger.info(f"  Grade D: {(df['ml_grade'] == 'D').sum()} records")
    
    return df[["match_id", "ml_grade", "ml_confidence"]]


def update_database(conn, table_name, grades_df: pd.DataFrame) -> int:
    """Update database with calculated grades and confidence."""
    if len(grades_df) == 0:
        logger.warning(f"No records to update in {table_name}")
        return 0
    
    logger.info(f"\nUpdating {len(grades_df)} records in {table_name}...")
    
    cursor = conn.cursor()
    
    update_query = f"""
    UPDATE {table_name}
    SET 
        ml_grade = %s,
        ml_confidence = %s
    WHERE match_id = %s
    """
    
    # Prepare data for batch update
    # Convert match_id to handle numeric(10,2) format
    update_data = [
        (row["ml_grade"], float(row["ml_confidence"]), float(row["match_id"]))
        for _, row in grades_df.iterrows()
    ]
    
    try:
        # Execute batch update
        execute_batch(cursor, update_query, update_data, page_size=BATCH_SIZE)
        conn.commit()
        logger.info(f"[OK] Successfully updated {len(grades_df)} records in {table_name}")
        return len(grades_df)
    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] Error updating {table_name}: {e}")
        raise
    finally:
        cursor.close()


def print_summary(table_name, grades_df: pd.DataFrame):
    """Print summary statistics."""
    logger.info("\n" + "=" * 80)
    logger.info(f"SUMMARY - {table_name}")
    logger.info("=" * 80)
    logger.info(f"Records updated: {len(grades_df)}")
    logger.info(f"\nGrade Distribution:")
    for grade in ["A", "B", "C", "D"]:
        count = (grades_df["ml_grade"] == grade).sum()
        pct = (count / len(grades_df)) * 100 if len(grades_df) > 0 else 0
        logger.info(f"  Grade {grade}: {count:>5} ({pct:>5.1f}%)")
    
    logger.info(f"\nConfidence Statistics:")
    logger.info(f"  Min: {grades_df['ml_confidence'].min():.2f}%")
    logger.info(f"  Max: {grades_df['ml_confidence'].max():.2f}%")
    logger.info(f"  Mean: {grades_df['ml_confidence'].mean():.2f}%")
    logger.info(f"  Median: {grades_df['ml_confidence'].median():.2f}%")
    
    logger.info(f"\nThresholds (in percentage):")
    logger.info(f"  A: ml_confidence < {Q1_THRESHOLD*100:.2f}%")
    logger.info(f"  B: {Q1_THRESHOLD*100:.2f}% <= ml_confidence < {Q2_THRESHOLD*100:.2f}%")
    logger.info(f"  C: {Q2_THRESHOLD*100:.2f}% <= ml_confidence < {Q3_THRESHOLD*100:.2f}%")
    logger.info(f"  D: ml_confidence >= {Q3_THRESHOLD*100:.2f}%")
    logger.info("=" * 80)


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("DATABASE GRADE UPDATE - DUAL TABLES")
    logger.info("=" * 80)
    logger.info(f"Tables: {', '.join(TABLE_NAMES)}")
    logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    conn = None
    try:
        # Connect to database
        conn = get_db_connection()
        
        # Process each table
        for table_name in TABLE_NAMES:
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing: {table_name}")
            logger.info(f"{'='*80}")
            
            # Fetch records with NULL ml_grade and non-NULL home_odds
            df_records = fetch_null_grade_records(conn, table_name)
            
            if len(df_records) == 0:
                logger.info(f"No records with NULL ml_grade and non-NULL home_odds found in {table_name}. Skipping.")
                continue
            
            # Calculate grades
            df_grades = calculate_grades(df_records)
            
            # Update database
            updated_count = update_database(conn, table_name, df_grades)
            
            # Print summary
            print_summary(table_name, df_grades)
        
        logger.info("\n" + "=" * 80)
        logger.info("[OK] Process completed successfully for all tables")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"[ERROR] Process failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    main()
