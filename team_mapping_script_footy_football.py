"""
STEP 2 — Run ONCE per season to build team_mapping table.

Gets team names from your agility_soccer_v1 (FootyStats names) and
Football API (api-sports.io), fuzzy-matches them, saves to team_mapping.

Usage (local with .env):
    python 02_build_team_mapping.py

Usage (GitHub Actions — secrets are injected as env vars automatically):
    python 02_build_team_mapping.py

After running → fix any unmatched teams with the SQL it prints.
"""
import os
import sys
import requests
import time
import psycopg2
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

# Load .env if it exists (local dev), ignored on GitHub Actions
load_dotenv()

# ── DB Connection ─────────────────────────────────────────────
def get_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        database=os.environ["DB_DATABASE"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode="require",
    )

# ── Config ────────────────────────────────────────────────────
FOOTBALL_API_KEY = os.environ["FOOTBALL_API_KEY"]
FOOTBALL_API_BASE = "https://v3.football.api-sports.io"

# ============================================================
# YOUR LEAGUES (from your frontend screenshot)
#
# left  = Football API league ID
# right = league_name as it appears in agility_soccer_v1
#
# Verify yours: SELECT DISTINCT league_name FROM agility_soccer_v1;
# ============================================================
LEAGUES = {
    39:   "England Premier League",
    40:   "England Championship",
    140:  "Spain La Liga",
    61:   "France Ligue 1",
    135:  "Italy Serie A",
    78:   "Germany Bundesliga",
    2:    "UEFA Champions League",
    3:    "UEFA Europa League",
    1:    "FIFA World Cup",
    15:   "FIFA Club World Cup",
    88:   "Netherlands Eredivisie",
    94:   "Portugal Liga NOS",
    262:  "Mexico Liga MX",
    71:   "Brazil Serie A",
    253:  "USA MLS",
}

SEASON = 2025


# ── Football API ──────────────────────────────────────────────
def get_football_api_teams(league_id):
    """Fetch teams from Football API for a league + season."""
    time.sleep(0.5)  # rate limit
    resp = requests.get(
        f"{FOOTBALL_API_BASE}/teams",
        headers={"x-apisports-key": FOOTBALL_API_KEY},
        params={"league": league_id, "season": SEASON},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    # If no results for 2025, try 2024 (some leagues use season start year)
    if not data.get("response"):
        print(f"     No teams for season {SEASON}, trying {SEASON - 1}...")
        time.sleep(0.5)
        resp = requests.get(
            f"{FOOTBALL_API_BASE}/teams",
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            params={"league": league_id, "season": SEASON - 1},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

    return [
        {"id": t["team"]["id"], "name": t["team"]["name"]}
        for t in data.get("response", [])
    ]


# ── Your Database ─────────────────────────────────────────────
def get_footystats_teams(conn, league_name):
    """Get unique team names from agility_soccer_v1 for a league."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT name FROM (
            SELECT home_team AS name FROM agility_soccer_v1 WHERE league_name = %s
            UNION
            SELECT away_team AS name FROM agility_soccer_v1 WHERE league_name = %s
        ) t WHERE name IS NOT NULL ORDER BY name
    """, (league_name, league_name))
    return [{"name": row[0]} for row in cur.fetchall()]


# ── Fuzzy Matching ────────────────────────────────────────────
def fuzzy_match(fa_teams, fs_teams, threshold=80):
    """Match FootyStats names → Football API names."""
    matched, unmatched = [], []
    used = set()

    for fs in fs_teams:
        best, best_score = None, 0
        for fa in fa_teams:
            if fa["id"] in used:
                continue
            score = fuzz.token_sort_ratio(fs["name"], fa["name"])
            if score > best_score:
                best_score = score
                best = fa

        if best and best_score >= threshold:
            matched.append({
                "footy_stats_name": fs["name"],
                "football_api_name": best["name"],
                "football_api_team_id": best["id"],
            })
            used.add(best["id"])
        else:
            unmatched.append({
                "footy_stats_name": fs["name"],
                "best_guess": best["name"] if best else "???",
                "best_guess_id": best["id"] if best else None,
                "score": best_score,
            })

    return matched, unmatched


# ── Save to DB ────────────────────────────────────────────────
def save_mappings(conn, mappings, league_name):
    cur = conn.cursor()
    for m in mappings:
        cur.execute("""
            INSERT INTO team_mapping
                (canonical_name, footy_stats_name, football_api_name,
                 football_api_team_id, league)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            m["football_api_name"],
            m["footy_stats_name"],
            m["football_api_name"],
            m["football_api_team_id"],
            league_name,
        ))
    conn.commit()


# ── Main ──────────────────────────────────────────────────────
def main():
    conn = get_connection()
    total_matched, total_unmatched = 0, 0
    all_unmatched = []

    # First, show what leagues exist in your DB
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT league_name FROM agility_soccer_v1 ORDER BY league_name")
    db_leagues = [row[0] for row in cur.fetchall()]
    print("Leagues in your DB:")
    for l in db_leagues:
        mapped = "✅" if l in LEAGUES.values() else "⚠️  NOT IN SCRIPT"
        print(f"  {mapped} {l}")
    print()

    for league_id, league_name in LEAGUES.items():
        print(f"\n{'='*60}")
        print(f"  {league_name}  (api-football id={league_id})")
        print(f"{'='*60}")

        fa_teams = get_football_api_teams(league_id)
        fs_teams = get_footystats_teams(conn, league_name)

        if not fa_teams:
            print(f"  ⚠️  No teams from Football API — check league_id={league_id}")
            continue
        if not fs_teams:
            print(f"  ⚠️  No teams in agility_soccer_v1 for '{league_name}'")
            continue

        print(f"  Football API: {len(fa_teams)} teams | FootyStats: {len(fs_teams)} teams")

        matched, unmatched = fuzzy_match(fa_teams, fs_teams)
        save_mappings(conn, matched, league_name)

        print(f"\n  ✅ Matched: {len(matched)}")
        for m in matched:
            marker = "" if m["footy_stats_name"] == m["football_api_name"] else " ⚡ NAME DIFFERS"
            print(f"     {m['footy_stats_name']:40s} → {m['football_api_name']}{marker}")

        if unmatched:
            print(f"\n  ❌ Unmatched: {len(unmatched)} — FIX MANUALLY")
            for u in unmatched:
                print(f"     {u['footy_stats_name']:40s} → guess: {u['best_guess']} ({u['score']}%)")
                all_unmatched.append({**u, "league": league_name})

        total_matched += len(matched)
        total_unmatched += len(unmatched)

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"  DONE — Matched: {total_matched}  |  Unmatched: {total_unmatched}")
    print(f"{'='*60}")

    if all_unmatched:
        print(f"\n  👇 SQL to fix unmatched teams (replace <CORRECT_NAME>):\n")
        for u in all_unmatched:
            fs_name = u["footy_stats_name"].replace("'", "''")
            guess = u["best_guess"].replace("'", "''")
            print(f"  INSERT INTO team_mapping (canonical_name, footy_stats_name, football_api_name, football_api_team_id, league)")
            print(f"  VALUES ('{guess}', '{fs_name}', '<CORRECT_NAME>', {u['best_guess_id'] or 'NULL'}, '{u['league']}');")
            print()

    conn.close()
    print("✅ team_mapping table populated.")


if __name__ == "__main__":
    main()
