import requests
import json

API_KEY = "1eac22f8ec8e6da731a49adeae1148f14d6ceca13db5a9ffba65618f97406f4e"
URL = f"https://api.football-data-api.com/league-list?key={API_KEY}"

# Target leagues - exact names to match (must match exactly to avoid false positives)
# We also keep a set of names to EXCLUDE even if they partial-match
EXACT_TARGETS = [
    ("England Premier League", "England"),
    ("Spain La Liga", "Spain"),
    ("Italy Serie A", "Italy"),
    ("Germany Bundesliga", "Germany"),
    ("USA Major League Soccer", "USA"),
    ("France Ligue 1", "France"),
    ("Netherlands Eredivisie", "Netherlands"),
    ("Mexico Liga MX", "Mexico"),
    ("UEFA Champions League", ""),
    ("Portugal Liga NOS", "Portugal"),
    ("Portugal Primeira Liga", "Portugal"),
]

# Keywords for fuzzy matching
KEYWORD_MAP = {
    "premier league": ("England", "England Premier League"),
    "la liga": ("Spain", "Spain La Liga"),
    "serie a": ("Italy", "Italy Serie A"),
    "bundesliga": ("Germany", "Germany Bundesliga"),
    "major league soccer": ("USA", "USA MLS"),
    "mls": ("USA", "USA MLS"),
    "ligue 1": ("France", "France Ligue 1"),
    "eredivisie": ("Netherlands", "Netherlands Eredivisie"),
    "liga mx": ("Mexico", "Mexico Liga MX"),
    "champions league": ("", "UEFA Champions League"),
    "liga nos": ("Portugal", "Portugal Liga NOS"),
    "primeira liga": ("Portugal", "Portugal Liga NOS"),
}

# Words that disqualify a match (to skip women's, youth, summer, etc.)
EXCLUDE_WORDS = ["women", "u18", "u21", "u23", "summer", "cup", "division two", "northern"]

# Seasons we want (current + newest/upcoming)
TARGET_SEASONS = {
    "2025", "2026", "2027",
    "20242025", "20252026", "20262027",
}


def matches_target(league_name, country=""):
    """Check if a league entry matches one of our 10 target leagues. Returns label or None."""
    name_lower = league_name.lower().strip()
    country_lower = country.lower().strip() if country else ""

    # Exclude youth / women / cup / etc
    for excl in EXCLUDE_WORDS:
        if excl in name_lower:
            return None

    for keyword, (expected_country, label) in KEYWORD_MAP.items():
        if keyword in name_lower:
            # Country check for ambiguous names
            ec = expected_country.lower()
            if ec and ec not in country_lower and ec not in name_lower:
                continue
            return label

    return None


def main():
    print("Fetching league list from FootyStats API...\n")
    response = requests.get(URL)
    response.raise_for_status()
    data = response.json()

    # FootyStats wraps response in {"success": true, "data": [...]}
    if isinstance(data, dict):
        leagues = data.get("data", [])
        if not isinstance(leagues, list):
            leagues = [data]
    else:
        leagues = data

    print(f"Total league entries returned: {len(leagues)}\n")

    # Each league object has: name, country, and a nested list of season dicts
    # e.g. {'name': 'England Premier League', 'country': 'England', 'seasons': [{'id': 15050, 'year': 20252026}, ...]}
    # The seasons key might vary - let's detect it

    all_entries = []   # flattened: one row per league+season combo
    filtered = []      # only target seasons

    for league in leagues:
        name = league.get("name", league.get("league_name", ""))
        country = league.get("country", "")
        label = matches_target(name, country)

        if not label:
            continue

        # Find the nested seasons list - try common key names
        seasons_list = None
        for key in league.keys():
            val = league[key]
            if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and "id" in val[0]:
                seasons_list = val
                break

        if not seasons_list:
            continue

        for s in seasons_list:
            sid = s.get("id", "N/A")
            syear = str(s.get("year", s.get("season", "")))

            entry = {
                "label": label,
                "api_name": name,
                "id": sid,
                "season": syear,
                "country": country,
            }
            all_entries.append(entry)

            if syear in TARGET_SEASONS:
                filtered.append(entry)

    # Print ALL seasons for target leagues
    print("=" * 95)
    print("ALL SEASONS FOR TARGET LEAGUES:")
    print("=" * 95)
    print(f"{'Label':<30} {'API Name':<35} {'ID':<8} {'Season':<12} {'Country'}")
    print("-" * 95)
    for m in sorted(all_entries, key=lambda x: (x["label"], x["season"])):
        print(f"{m['label']:<30} {m['api_name']:<35} {m['id']:<8} {m['season']:<12} {m['country']}")

    # Print filtered newest seasons
    print(f"\n{'=' * 95}")
    print(f"NEWEST LEAGUE IDS (seasons: {', '.join(sorted(TARGET_SEASONS))})")
    print("=" * 95)
    print(f"{'Label':<30} {'API Name':<35} {'ID':<8} {'Season':<12} {'Country'}")
    print("-" * 95)

    if filtered:
        for m in sorted(filtered, key=lambda x: (x["label"], x["season"])):
            print(f"{m['label']:<30} {m['api_name']:<35} {m['id']:<8} {m['season']:<12} {m['country']}")
    else:
        print("No entries found matching target seasons.")

    print(f"\nTotal newest entries found: {len(filtered)}")

    # Quick reference
    if filtered:
        print(f"\n{'=' * 95}")
        print("QUICK REFERENCE - Latest IDs:")
        print("=" * 95)
        # Group by label, show highest season per label
        from collections import defaultdict
        by_label = defaultdict(list)
        for m in filtered:
            by_label[m["label"]].append(m)
        for label in sorted(by_label.keys()):
            entries = sorted(by_label[label], key=lambda x: x["season"], reverse=True)
            for e in entries:
                print(f"  {e['label']:<30} -> ID: {e['id']:<8} (season: {e['season']})")

    # Save
    output = {
        "all_target_leagues": all_entries,
        "newest_ids": filtered,
    }
    with open("league_ids_output.json", "w") as f:
        json.dump(output, f, indent=2)
    print("\nFull results saved to league_ids_output.json")


if __name__ == "__main__":
    main()
