"""
STEP 3 — Daily Match Mapper

Runs daily via GitHub Actions (or manually).
Fetches fixtures from Football API, finds matching predictions
in agility_soccer_v1, and saves the link to match_mapping.

Usage:
    python 03_daily_match_mapper.py                    # today + tomorrow
    python 03_daily_match_mapper.py --date 2026-02-25  # specific date
    python 03_daily_match_mapper.py --from 2026-02-20 --to 2026-02-28  # range
"""
import os
import argparse
import requests
import time
import psycopg2
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
from dotenv import load_dotenv

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

LEAGUES = {
    39:   "England Premier League",
    140:  "Spain La Liga",
    61:   "France Ligue 1",
    135:  "Italy Serie A",
    78:   "Germany Bundesliga",
    2:    "UEFA Champions League",
    88:   "Netherlands Eredivisie",
    94:   "Portugal Liga NOS",
    262:  "Mexico Liga MX",
    253:  "USA MLS",
}

SEASON = 2025


# ── Football API ──────────────────────────────────────────────
def get_fixtures(date_str, league_id):
    """Fetch fixtures from Football API for a date + league."""
    time.sleep(0.3)
    resp = requests.get(
        f"{FOOTBALL_API_BASE}/fixtures",
        headers={"x-apisports-key": FOOTBALL_API_KEY},
        params={"date": date_str, "league": league_id, "season": SEASON},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json().get("response", [])

    # If no results for 2025, try 2024 (European leagues use start year)
    if not data:
        time.sleep(0.3)
        resp = requests.get(
            f"{FOOTBALL_API_BASE}/fixtures",
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            params={"date": date_str, "league": league_id, "season": SEASON - 1},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json().get("response", [])

    return data


# ── Mapping Logic ─────────────────────────────────────────────
def resolve_team(cur, fa_name, league_name):
    """Look up FootyStats name from team_mapping."""
    # Try with league first
    cur.execute("""
        SELECT footy_stats_name FROM team_mapping
        WHERE football_api_name = %s AND league = %s
    """, (fa_name, league_name))
    row = cur.fetchone()
    if row:
        return row[0]

    # Fallback: without league (for cups/cross-league)
    cur.execute("""
        SELECT footy_stats_name FROM team_mapping
        WHERE football_api_name = %s LIMIT 1
    """, (fa_name,))
    row = cur.fetchone()
    return row[0] if row else None


def find_prediction(cur, fs_home, fs_away, match_date):
    """Find match_id in agility_soccer_v1 by team + date."""
    # Exact date
    cur.execute("""
        SELECT match_id FROM agility_soccer_v1
        WHERE home_team = %s AND away_team = %s AND date = %s
    """, (fs_home, fs_away, match_date))
    row = cur.fetchone()
    if row:
        return row[0]

    # ±1 day (timezone shifts)
    cur.execute("""
        SELECT match_id FROM agility_soccer_v1
        WHERE home_team = %s AND away_team = %s
        AND date BETWEEN (%s::date - 1) AND (%s::date + 1)
        ORDER BY ABS(date - %s::date) LIMIT 1
    """, (fs_home, fs_away, match_date, match_date, match_date))
    row = cur.fetchone()
    return row[0] if row else None


def fallback_fuzzy(cur, fa_home, fa_away, match_date):
    """OPTION 3 FALLBACK: fuzzy match directly against predictions."""
    cur.execute("""
        SELECT match_id, home_team, away_team FROM agility_soccer_v1
        WHERE date BETWEEN (%s::date - 1) AND (%s::date + 1)
    """, (match_date, match_date))
    candidates = cur.fetchall()

    best_id, best_score = None, 0
    for mid, h, a in candidates:
        score = (fuzz.token_sort_ratio(fa_home, h) + fuzz.token_sort_ratio(fa_away, a)) / 2
        if score > best_score:
            best_score = score
            best_id = mid

    return best_id if best_score >= 85 else None


# ── Core Mapper ───────────────────────────────────────────────
def map_single_date(conn, date_str):
    """Map all fixtures for one date."""
    cur = conn.cursor()
    stats = {"mapped": 0, "skipped": 0, "fallback": 0, "failed": []}

    for league_id, league_name in LEAGUES.items():
        fixtures = get_fixtures(date_str, league_id)

        for fix in fixtures:
            fa_id = fix["fixture"]["id"]
            fa_home = fix["teams"]["home"]["name"]
            fa_away = fix["teams"]["away"]["name"]
            m_date = fix["fixture"]["date"][:10]

            # Already mapped? skip
            cur.execute("SELECT 1 FROM match_mapping WHERE football_api_match_id = %s", (fa_id,))
            if cur.fetchone():
                stats["skipped"] += 1
                continue

            # ── OPTION 1: team_mapping lookup ──
            fs_home = resolve_team(cur, fa_home, league_name)
            fs_away = resolve_team(cur, fa_away, league_name)

            fs_match_id = None
            via = "auto"

            if fs_home and fs_away:
                fs_match_id = find_prediction(cur, fs_home, fs_away, m_date)

            # ── OPTION 3 FALLBACK ──
            if not fs_match_id:
                fs_match_id = fallback_fuzzy(cur, fa_home, fa_away, m_date)
                if fs_match_id:
                    via = "fallback"
                    stats["fallback"] += 1

            if fs_match_id:
                cur.execute("""
                    INSERT INTO match_mapping
                        (football_api_match_id, footy_stats_match_id,
                         home_team, away_team, match_date, league, mapped_via)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (football_api_match_id) DO NOTHING
                """, (fa_id, fs_match_id, fa_home, fa_away, m_date, league_name, via))
                stats["mapped"] += 1
            else:
                reason = "team_not_mapped" if (not fs_home or not fs_away) else "no_prediction"
                stats["failed"].append(f"{league_name}: {fa_home} vs {fa_away} ({reason})")

    conn.commit()
    return stats


def print_stats(date_str, stats):
    print(f"\n📅 {date_str}")
    print(f"   ✅ Mapped:   {stats['mapped']}")
    print(f"   🔄 Fallback: {stats['fallback']}")
    print(f"   ⏭️  Skipped:  {stats['skipped']} (already done)")
    if stats["failed"]:
        print(f"   ❌ Failed:   {len(stats['failed'])}")
        for f in stats["failed"]:
            print(f"      {f}")


# ── Main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Specific date YYYY-MM-DD")
    parser.add_argument("--from", dest="from_date", help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    conn = get_connection()

    if args.from_date and args.to_date:
        d = datetime.strptime(args.from_date, "%Y-%m-%d")
        end = datetime.strptime(args.to_date, "%Y-%m-%d")
        total_mapped, total_failed = 0, 0
        while d <= end:
            ds = d.strftime("%Y-%m-%d")
            stats = map_single_date(conn, ds)
            print_stats(ds, stats)
            total_mapped += stats["mapped"]
            total_failed += len(stats["failed"])
            d += timedelta(days=1)
        print(f"\n🏁 Range done — Mapped: {total_mapped} | Failed: {total_failed}")

    elif args.date:
        stats = map_single_date(conn, args.date)
        print_stats(args.date, stats)

    else:
        # Default: today + tomorrow
        today = datetime.utcnow().strftime("%Y-%m-%d")
        tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        for d in [today, tomorrow]:
            stats = map_single_date(conn, d)
            print_stats(d, stats)

    conn.close()
    print("\n✅ Done!")


if __name__ == "__main__":
    main()
