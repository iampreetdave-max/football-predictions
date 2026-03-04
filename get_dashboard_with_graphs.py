"""
generate_dashboards.py
======================
Connects to PostgreSQL, queries the prediction tables directly,
and generates self-contained HTML dashboards with data baked in.

Tables queried:
  - predictions_soccer_v1_ourmodel
  - predictions_soccer_v2_ourmodel
  - predictions_soccer_v3_ourmodel
  - predictions_nba_b1_ourmodel

Output:
  - soccer_model_dashboard.html
  - nba_model_b1_dashboard.html

Usage:
  pip install psycopg2-binary
  python generate_dashboards.py
"""

import json
import psycopg2
import psycopg2.extras
from datetime import datetime, date
from decimal import Decimal

# ──────────────────────────────────────────────────
# DATABASE CONNECTION — CHANGE THESE
# ──────────────────────────────────────────────────
DB_CONFIG = {
    "host": "winbets-predictions.postgres.database.azure.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "winbets",
    "password": "Bambam2389@",  # <-- ADD YOUR PASSWORD HERE
    "sslmode": "require",
}

# ──────────────────────────────────────────────────
# OUTPUT FILE PATHS
# ──────────────────────────────────────────────────
SOCCER_OUTPUT = "soccer_model_dashboard.html"
NBA_OUTPUT = "nba_model_b1_dashboard.html"


# ──────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────
def json_serializer(obj):
    """Handle date, datetime, Decimal for JSON serialization."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bool):
        return obj
    raise TypeError(f"Type {type(obj)} not serializable")


def query_table(cursor, table_name):
    """Query entire table and return list of dicts."""
    cursor.execute(f'SELECT * FROM "{table_name}"')
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    result = []
    for row in rows:
        d = {}
        for i, col in enumerate(columns):
            val = row[i]
            # Convert Decimal to float for JSON
            if isinstance(val, Decimal):
                val = float(val)
            # Convert date/datetime to string
            if isinstance(val, (datetime, date)):
                val = val.isoformat()
            # Convert boolean explicitly
            if isinstance(val, bool):
                val = val
            d[col] = val
        result.append(d)
    return result


def fetch_all_data():
    """Connect to PostgreSQL and fetch all 4 tables."""
    print(f"Connecting to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        sslmode=DB_CONFIG["sslmode"],
    )
    cursor = conn.cursor()

    data = {}

    soccer_tables = {
        "v1": "predictions_soccer_v1_ourmodel",
        "v2": "predictions_soccer_v2_ourmodel",
        "v3": "predictions_soccer_v3_ourmodel",
    }

    nba_tables = {
        "b1": "predictions_nba_b1_ourmodel",
    }

    for key, table in soccer_tables.items():
        print(f"  Querying {table}...")
        try:
            rows = query_table(cursor, table)
            data[f"soccer_{key}"] = rows
            print(f"    → {len(rows)} rows")
        except Exception as e:
            print(f"    ⚠ Error querying {table}: {e}")
            conn.rollback()
            data[f"soccer_{key}"] = []

    for key, table in nba_tables.items():
        print(f"  Querying {table}...")
        try:
            rows = query_table(cursor, table)
            data[f"nba_{key}"] = rows
            print(f"    → {len(rows)} rows")
        except Exception as e:
            print(f"    ⚠ Error querying {table}: {e}")
            conn.rollback()
            data[f"nba_{key}"] = []

    cursor.close()
    conn.close()
    print("Database connection closed.\n")
    return data


# ──────────────────────────────────────────────────
# SOCCER DASHBOARD HTML GENERATOR
# ──────────────────────────────────────────────────
def generate_soccer_html(v1_data, v2_data, v3_data):
    """Generate the soccer dashboard HTML with data baked in."""

    # Serialize data to JSON strings
    v1_json = json.dumps(v1_data, default=json_serializer)
    v2_json = json.dumps(v2_data, default=json_serializer)
    v3_json = json.dumps(v3_data, default=json_serializer)

    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Soccer Model Performance Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0a0e17; --surface: #111827; --surface2: #1a2235; --border: #1e2a3e;
  --text: #e2e8f0; --text-dim: #8899aa;
  --accent-v1: #f59e0b; --accent-v2: #3b82f6; --accent-v3: #10b981;
  --green: #22c55e; --red: #ef4444;
  --grade-a: #22c55e; --grade-b: #3b82f6; --grade-c: #f59e0b; --grade-d: #ef4444;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'DM Sans',sans-serif; background:var(--bg); color:var(--text); min-height:100vh; overflow-x:hidden; }
.header { padding:32px 40px 24px; border-bottom:1px solid var(--border); background:linear-gradient(180deg,#0f1520 0%,var(--bg) 100%); }
.header h1 { font-size:28px; font-weight:700; letter-spacing:-0.5px; background:linear-gradient(135deg,#f59e0b,#3b82f6,#10b981); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
.header p { color:var(--text-dim); font-size:14px; margin-top:6px; }
.header .gen-time { color:var(--text-dim); font-size:11px; margin-top:4px; font-family:'JetBrains Mono',monospace; }
.tabs { display:flex; gap:4px; padding:16px 40px 0; border-bottom:1px solid var(--border); }
.tab { padding:10px 20px; font-size:13px; font-weight:600; color:var(--text-dim); cursor:pointer; border-bottom:2px solid transparent; transition:all 0.2s; font-family:'DM Sans',sans-serif; background:none; border-top:none; border-left:none; border-right:none; position:relative; top:1px; }
.tab:hover { color:var(--text); }
.tab.active-v1 { color:var(--accent-v1); border-bottom-color:var(--accent-v1); }
.tab.active-v2 { color:var(--accent-v2); border-bottom-color:var(--accent-v2); }
.tab.active-v3 { color:var(--accent-v3); border-bottom-color:var(--accent-v3); }
.tab.active-all { color:#fff; border-bottom-color:#fff; }
.tab .dot { display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:6px; }
.dot-v1 { background:var(--accent-v1); } .dot-v2 { background:var(--accent-v2); } .dot-v3 { background:var(--accent-v3); }
.dot-all { background:linear-gradient(135deg,var(--accent-v1),var(--accent-v2),var(--accent-v3)); }
.content { padding:28px 40px 60px; }
.panel { display:none; } .panel.active { display:block; }
.info-banner { background:var(--surface2); border:1px solid var(--border); border-left:3px solid var(--accent-v2); border-radius:8px; padding:12px 18px; margin-bottom:20px; font-size:13px; color:var(--text-dim); line-height:1.5; }
.info-banner strong { color:var(--text); }
.kpi-row { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin-bottom:28px; }
.kpi { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px; position:relative; overflow:hidden; }
.kpi::before { content:''; position:absolute; top:0; left:0; right:0; height:3px; }
.kpi-v1::before { background:var(--accent-v1); } .kpi-v2::before { background:var(--accent-v2); } .kpi-v3::before { background:var(--accent-v3); }
.kpi-label { font-size:11px; text-transform:uppercase; letter-spacing:1px; color:var(--text-dim); font-weight:600; }
.kpi-value { font-family:'JetBrains Mono',monospace; font-size:28px; font-weight:700; margin-top:8px; }
.kpi-sub { font-size:12px; color:var(--text-dim); margin-top:4px; font-family:'JetBrains Mono',monospace; }
.positive { color:var(--green); } .negative { color:var(--red); }
.chart-grid { display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-bottom:28px; }
.chart-grid.triple { grid-template-columns:1fr 1fr 1fr; }
.chart-card { background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:24px; }
.chart-card.full { grid-column:1/-1; }
.chart-title { font-size:14px; font-weight:600; margin-bottom:16px; display:flex; align-items:center; gap:8px; }
.badge { font-size:10px; padding:2px 8px; border-radius:99px; font-weight:600; text-transform:uppercase; letter-spacing:0.5px; }
.badge-ml { background:rgba(59,130,246,0.15); color:#60a5fa; } .badge-ou { background:rgba(16,185,129,0.15); color:#34d399; }
.chart-container { position:relative; height:280px; } .chart-container.tall { height:350px; }
.data-table { width:100%; border-collapse:collapse; font-size:13px; }
.data-table th { text-align:left; padding:10px 14px; font-size:11px; text-transform:uppercase; letter-spacing:0.8px; color:var(--text-dim); border-bottom:1px solid var(--border); font-weight:600; }
.data-table td { padding:10px 14px; border-bottom:1px solid rgba(30,42,62,0.5); font-family:'JetBrains Mono',monospace; font-size:12px; }
.data-table tr:hover td { background:rgba(255,255,255,0.02); }
.grade-pill { display:inline-block; padding:2px 10px; border-radius:6px; font-weight:600; font-size:11px; }
.grade-A { background:rgba(34,197,94,0.15); color:#22c55e; } .grade-B { background:rgba(59,130,246,0.15); color:#60a5fa; }
.grade-C { background:rgba(245,158,11,0.15); color:#f59e0b; } .grade-D { background:rgba(239,68,68,0.15); color:#ef4444; }
@media (max-width:900px) { .chart-grid,.chart-grid.triple { grid-template-columns:1fr; } .kpi-row { grid-template-columns:repeat(2,1fr); } .content,.header,.tabs { padding-left:20px; padding-right:20px; } }
</style>
</head>
<body>

<div class="header">
  <h1>Soccer Prediction Model Analytics</h1>
  <p>Performance comparison across V1, V2, V3 — connected directly to PostgreSQL</p>
  <div class="gen-time">Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + r"""</div>
</div>

<div class="tabs">
  <button class="tab active-all" data-tab="compare" onclick="switchTab(this)"><span class="dot dot-all"></span>Compare All</button>
  <button class="tab" data-tab="v1" onclick="switchTab(this)"><span class="dot dot-v1"></span>Model V1</button>
  <button class="tab" data-tab="v2" onclick="switchTab(this)"><span class="dot dot-v2"></span>Model V2</button>
  <button class="tab" data-tab="v3" onclick="switchTab(this)"><span class="dot dot-v3"></span>Model V3</button>
</div>

<div class="content">
  <div id="dashboard">
    <div class="panel active" id="panel-compare">
      <div id="compare-banner"></div>
      <div class="kpi-row" id="kpi-compare"></div>
      <div class="chart-grid" id="charts-compare-top"></div>
      <div class="chart-grid triple" id="charts-compare-grades"></div>
      <div class="chart-grid" id="charts-compare-bottom"></div>
    </div>
    <div class="panel" id="panel-v1">
      <div class="kpi-row" id="kpi-v1"></div>
      <div class="chart-grid" id="charts-v1-top"></div>
      <div class="chart-grid" id="charts-v1-bottom"></div>
      <div class="chart-card" id="table-v1" style="margin-top:20px"></div>
    </div>
    <div class="panel" id="panel-v2">
      <div class="kpi-row" id="kpi-v2"></div>
      <div class="chart-grid" id="charts-v2-top"></div>
      <div class="chart-grid" id="charts-v2-bottom"></div>
      <div class="chart-card" id="table-v2" style="margin-top:20px"></div>
    </div>
    <div class="panel" id="panel-v3">
      <div class="kpi-row" id="kpi-v3"></div>
      <div class="chart-grid" id="charts-v3-top"></div>
      <div class="chart-grid" id="charts-v3-bottom"></div>
      <div class="chart-card" id="table-v3" style="margin-top:20px"></div>
    </div>
  </div>
</div>

<script>
// ─── DATA BAKED IN FROM POSTGRESQL ───
const DATA = {
  v1: """ + v1_json + r""",
  v2: """ + v2_json + r""",
  v3: """ + v3_json + r"""
};

const CHARTS = {};
const COLORS = { v1:'#f59e0b', v2:'#3b82f6', v3:'#10b981', green:'#22c55e', red:'#ef4444', gradeA:'#22c55e', gradeB:'#3b82f6', gradeC:'#f59e0b', gradeD:'#ef4444' };

Chart.defaults.color = '#8899aa';
Chart.defaults.borderColor = '#1e2a3e';
Chart.defaults.font.family = "'DM Sans', sans-serif";
Chart.defaults.font.size = 11;
Chart.defaults.plugins.legend.labels.boxWidth = 12;
Chart.defaults.plugins.legend.labels.padding = 16;

function stl(d) { return d.filter(r => r.status === 'SETTLED'); }
function pct(n) { return (n*100).toFixed(1)+'%'; }
function fmt(n) { return (n>=0?'+':'')+n.toFixed(2); }
function nGrade(g) { if(!g) return 'D'; const u=String(g).toUpperCase().trim(); if(u.startsWith('A')) return 'A'; if(u.startsWith('B')) return 'B'; if(u.startsWith('C')) return 'C'; return 'D'; }
function gCol(g) { return {A:COLORS.gradeA,B:COLORS.gradeB,C:COLORS.gradeC,D:COLORS.gradeD}[g]||COLORS.gradeD; }

function gameId(r) {
  if (r.universal_game_id!=null && r.universal_game_id!=='') return String(r.universal_game_id);
  if (r.match_id!=null && r.match_id!=='') return 'mid_'+String(r.match_id);
  return null;
}

function commonSettledIds() {
  const vers = ['v1','v2','v3'].filter(v => DATA[v] && DATA[v].length > 0);
  if (vers.length <= 1) return null;
  const sets = vers.map(v => { const s=new Set(); stl(DATA[v]).forEach(r=>{const id=gameId(r);if(id)s.add(id);}); return s; });
  let common = sets[0];
  for (let i=1;i<sets.length;i++) { const nx=new Set(); common.forEach(id=>{if(sets[i].has(id))nx.add(id);}); common=nx; }
  return common;
}

function mapWinner(r) {
  if(!r.actual_winner) return null;
  if(r.actual_winner==='Draw') return 'Draw';
  if(r.actual_winner===r.home_team) return 'Home Win';
  if(r.actual_winner===r.away_team) return 'Away Win';
  return null;
}

function v1Stats(data,fids) {
  let s=stl(data); if(fids) s=s.filter(r=>{const id=gameId(r);return id&&fids.has(id);});
  const total=s.length; let mlC=0,mlT=0,ouC=0,ouT=0;
  s.forEach(r=>{if(!r.predicted_winner)return;const a=mapWinner(r);if(!a)return;mlT++;if(r.predicted_winner===a)mlC++;});
  s.forEach(r=>{if(!r.predicted_outcome||!r.actual_over_under)return;ouT++;if(r.predicted_outcome===r.actual_over_under)ouC++;});
  const mlPnl=s.reduce((a,r)=>a+(r.profit_loss_winner||0),0), ouPnl=s.reduce((a,r)=>a+(r.profit_loss_outcome||0),0);
  const mlGrades={},ouGrades={};
  s.forEach(r=>{if(r.ml_grade){const g=nGrade(r.ml_grade);if(!mlGrades[g])mlGrades[g]={correct:0,total:0,pnl:0};mlGrades[g].total++;const a=mapWinner(r);if(r.predicted_winner===a)mlGrades[g].correct++;mlGrades[g].pnl+=r.profit_loss_winner||0;}if(r.ou_grade){const g=nGrade(r.ou_grade);if(!ouGrades[g])ouGrades[g]={correct:0,total:0,pnl:0};ouGrades[g].total++;if(r.predicted_outcome===r.actual_over_under)ouGrades[g].correct++;ouGrades[g].pnl+=r.profit_loss_outcome||0;}});
  const leagues={};
  s.forEach(r=>{const lg=r.league_name||r.league||'Unknown';if(!leagues[lg])leagues[lg]={mlC:0,mlT:0,ouC:0,ouT:0,mlPnl:0,ouPnl:0};const a=mapWinner(r);if(r.predicted_winner&&a){leagues[lg].mlT++;if(r.predicted_winner===a)leagues[lg].mlC++;}if(r.predicted_outcome&&r.actual_over_under){leagues[lg].ouT++;if(r.predicted_outcome===r.actual_over_under)leagues[lg].ouC++;}leagues[lg].mlPnl+=r.profit_loss_winner||0;leagues[lg].ouPnl+=r.profit_loss_outcome||0;});
  const confLevels={};
  s.forEach(r=>{const cl=(r.confidence_category||'').toUpperCase();if(!cl)return;if(!confLevels[cl])confLevels[cl]={mlC:0,mlT:0,ouC:0,ouT:0};const a=mapWinner(r);if(r.predicted_winner&&a){confLevels[cl].mlT++;if(r.predicted_winner===a)confLevels[cl].mlC++;}if(r.predicted_outcome&&r.actual_over_under){confLevels[cl].ouT++;if(r.predicted_outcome===r.actual_over_under)confLevels[cl].ouC++;}});
  return{total,mlAcc:mlT?mlC/mlT:0,ouAcc:ouT?ouC/ouT:0,mlPnl,ouPnl,mlTotal:mlT,ouTotal:ouT,mlROI:mlT?(mlPnl/mlT)*100:0,ouROI:ouT?(ouPnl/ouT)*100:0,mlGrades,ouGrades,leagues,confLevels};
}

function v2Stats(data,fids) {
  let s=stl(data); if(fids) s=s.filter(r=>{const id=gameId(r);return id&&fids.has(id);});
  const total=s.length;
  const mlV=s.filter(r=>r.ml_correct!==null&&r.ml_correct!==undefined&&r.ml_correct!=='');
  const ouV=s.filter(r=>r.ou_correct!==null&&r.ou_correct!==undefined&&r.ou_correct!=='');
  const mlAcc=mlV.length?mlV.reduce((a,r)=>a+(r.ml_correct?1:0),0)/mlV.length:0;
  const ouAcc=ouV.length?ouV.reduce((a,r)=>a+(r.ou_correct?1:0),0)/ouV.length:0;
  const mlPnl=s.reduce((a,r)=>a+(r.ml_pnl||0),0), ouPnl=s.reduce((a,r)=>a+(r.ou_pnl||0),0);
  const mlGrades={},ouGrades={};
  s.forEach(r=>{if(r.ml_grade&&r.ml_correct!==null&&r.ml_correct!==undefined&&r.ml_correct!==''){const g=nGrade(r.ml_grade);if(!mlGrades[g])mlGrades[g]={correct:0,total:0,pnl:0};mlGrades[g].total++;if(r.ml_correct)mlGrades[g].correct++;mlGrades[g].pnl+=r.ml_pnl||0;}if(r.ou_grade&&r.ou_correct!==null&&r.ou_correct!==undefined&&r.ou_correct!==''){const g=nGrade(r.ou_grade);if(!ouGrades[g])ouGrades[g]={correct:0,total:0,pnl:0};ouGrades[g].total++;if(r.ou_correct)ouGrades[g].correct++;ouGrades[g].pnl+=r.ou_pnl||0;}});
  const leagues={};
  s.forEach(r=>{const lg=r.league||'Unknown';if(!leagues[lg])leagues[lg]={mlC:0,mlT:0,ouC:0,ouT:0,mlPnl:0,ouPnl:0};if(r.ml_correct!==null&&r.ml_correct!==undefined&&r.ml_correct!==''){leagues[lg].mlT++;if(r.ml_correct)leagues[lg].mlC++;}if(r.ou_correct!==null&&r.ou_correct!==undefined&&r.ou_correct!==''){leagues[lg].ouT++;if(r.ou_correct)leagues[lg].ouC++;}leagues[lg].mlPnl+=r.ml_pnl||0;leagues[lg].ouPnl+=r.ou_pnl||0;});
  const confLevels={};
  s.forEach(r=>{const cl=(r.ml_confidence_level||'').toUpperCase();if(!cl)return;if(!confLevels[cl])confLevels[cl]={mlC:0,mlT:0,ouC:0,ouT:0};if(r.ml_correct!==null&&r.ml_correct!==undefined&&r.ml_correct!==''){confLevels[cl].mlT++;if(r.ml_correct)confLevels[cl].mlC++;}if(r.ou_correct!==null&&r.ou_correct!==undefined&&r.ou_correct!==''){confLevels[cl].ouT++;if(r.ou_correct)confLevels[cl].ouC++;}});
  return{total,mlAcc,ouAcc,mlPnl,ouPnl,mlTotal:mlV.length,ouTotal:ouV.length,mlROI:mlV.length?(mlPnl/mlV.length)*100:0,ouROI:ouV.length?(ouPnl/ouV.length)*100:0,mlGrades,ouGrades,leagues,confLevels};
}

function v3Stats(data,fids) {
  let s=stl(data); if(fids) s=s.filter(r=>{const id=gameId(r);return id&&fids.has(id);});
  const total=s.length; let mlC=0,mlT=0,ouC=0,ouT=0;
  s.forEach(r=>{if(!r.predicted_winner)return;const a=mapWinner(r);if(!a)return;mlT++;if(r.predicted_winner===a)mlC++;});
  s.forEach(r=>{if(!r.predicted_over_under||!r.actual_over_under)return;ouT++;if(r.predicted_over_under===r.actual_over_under)ouC++;});
  const mlPnl=s.reduce((a,r)=>a+(r.profit_loss_winner||0),0), ouPnl=s.reduce((a,r)=>a+(r.profit_loss_over_under||0),0);
  const mlGrades={},ouGrades={};
  s.forEach(r=>{if(r.ml_grade){const g=nGrade(r.ml_grade);if(!mlGrades[g])mlGrades[g]={correct:0,total:0,pnl:0};mlGrades[g].total++;const a=mapWinner(r);if(r.predicted_winner===a)mlGrades[g].correct++;mlGrades[g].pnl+=r.profit_loss_winner||0;}if(r.ou_grade){const g=nGrade(r.ou_grade);if(!ouGrades[g])ouGrades[g]={correct:0,total:0,pnl:0};ouGrades[g].total++;if(r.predicted_over_under===r.actual_over_under)ouGrades[g].correct++;ouGrades[g].pnl+=r.profit_loss_over_under||0;}});
  const leagues={};
  s.forEach(r=>{const lg=r.league_name||r.league||'Unknown';if(!leagues[lg])leagues[lg]={mlC:0,mlT:0,ouC:0,ouT:0,mlPnl:0,ouPnl:0};const a=mapWinner(r);if(r.predicted_winner&&a){leagues[lg].mlT++;if(r.predicted_winner===a)leagues[lg].mlC++;}if(r.predicted_over_under&&r.actual_over_under){leagues[lg].ouT++;if(r.predicted_over_under===r.actual_over_under)leagues[lg].ouC++;}leagues[lg].mlPnl+=r.profit_loss_winner||0;leagues[lg].ouPnl+=r.profit_loss_over_under||0;});
  const confLevels={};
  s.forEach(r=>{const cl=(r.confidence_category||'').toUpperCase();if(!cl)return;if(!confLevels[cl])confLevels[cl]={mlC:0,mlT:0,ouC:0,ouT:0};const a=mapWinner(r);if(r.predicted_winner&&a){confLevels[cl].mlT++;if(r.predicted_winner===a)confLevels[cl].mlC++;}if(r.predicted_over_under&&r.actual_over_under){confLevels[cl].ouT++;if(r.predicted_over_under===r.actual_over_under)confLevels[cl].ouC++;}});
  return{total,mlAcc:mlT?mlC/mlT:0,ouAcc:ouT?ouC/ouT:0,mlPnl,ouPnl,mlTotal:mlT,ouTotal:ouT,mlROI:mlT?(mlPnl/mlT)*100:0,ouROI:ouT?(ouPnl/ouT)*100:0,mlGrades,ouGrades,leagues,confLevels};
}

// ─── INIT: build on page load ───
const cids = commonSettledIds();
const fn = { v1:v1Stats, v2:v2Stats, v3:v3Stats };
const cmp = {}, full = {};
const vers = ['v1','v2','v3'].filter(v => DATA[v] && DATA[v].length > 0);
vers.forEach(v => { cmp[v]=fn[v](DATA[v],cids); full[v]=fn[v](DATA[v],null); });

buildCompare(cmp, cids, full);
vers.forEach(v => buildModel(v, full[v]));

function buildCompare(stats, cids, full) {
  const vers = Object.keys(stats);
  const banner = document.getElementById('compare-banner');
  if (cids && vers.length > 1) {
    const fc = vers.map(v=>v.toUpperCase()+': '+full[v].total).join(', ');
    banner.innerHTML = '<div class="info-banner"><strong>Fair comparison:</strong> Showing only the <strong>'+cids.size+' common games</strong> settled across all loaded models. Full settled counts — '+fc+'.</div>';
  }

  let kh='';
  vers.forEach(v=>{const s=stats[v],cl='kpi-'+v;
    kh+='<div class="kpi '+cl+'"><div class="kpi-label">'+v.toUpperCase()+' ML Accuracy</div><div class="kpi-value">'+pct(s.mlAcc)+'</div><div class="kpi-sub">'+s.mlTotal+' predictions</div></div>';
    kh+='<div class="kpi '+cl+'"><div class="kpi-label">'+v.toUpperCase()+' O/U Accuracy</div><div class="kpi-value">'+pct(s.ouAcc)+'</div><div class="kpi-sub">'+s.ouTotal+' predictions</div></div>';
    kh+='<div class="kpi '+cl+'"><div class="kpi-label">'+v.toUpperCase()+' ML ROI</div><div class="kpi-value '+(s.mlROI>=0?'positive':'negative')+'">'+s.mlROI.toFixed(1)+'%</div><div class="kpi-sub">PnL: '+fmt(s.mlPnl)+' units</div></div>';
  });
  document.getElementById('kpi-compare').innerHTML = kh;

  const topEl=document.getElementById('charts-compare-top');
  topEl.innerHTML='<div class="chart-card"><div class="chart-title">Accuracy Comparison <span class="badge badge-ml">ML</span><span class="badge badge-ou">O/U</span></div><div class="chart-container"><canvas id="cmp-acc"></canvas></div></div><div class="chart-card"><div class="chart-title">ROI Comparison</div><div class="chart-container"><canvas id="cmp-roi"></canvas></div></div>';
  const lbl=vers.map(v=>v.toUpperCase());
  CHARTS['cmp-acc']=new Chart(document.getElementById('cmp-acc'),{type:'bar',data:{labels:lbl,datasets:[{label:'ML Accuracy',data:vers.map(v=>(stats[v].mlAcc*100).toFixed(1)),backgroundColor:'rgba(59,130,246,0.7)',borderRadius:6,barPercentage:0.6},{label:'O/U Accuracy',data:vers.map(v=>(stats[v].ouAcc*100).toFixed(1)),backgroundColor:'rgba(16,185,129,0.7)',borderRadius:6,barPercentage:0.6}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,max:80,ticks:{callback:v=>v+'%'}}}}});
  CHARTS['cmp-roi']=new Chart(document.getElementById('cmp-roi'),{type:'bar',data:{labels:lbl,datasets:[{label:'ML ROI',data:vers.map(v=>stats[v].mlROI.toFixed(2)),backgroundColor:vers.map(v=>stats[v].mlROI>=0?'rgba(34,197,94,0.7)':'rgba(239,68,68,0.7)'),borderRadius:6,barPercentage:0.6},{label:'O/U ROI',data:vers.map(v=>stats[v].ouROI.toFixed(2)),backgroundColor:vers.map(v=>stats[v].ouROI>=0?'rgba(34,197,94,0.4)':'rgba(239,68,68,0.4)'),borderRadius:6,barPercentage:0.6}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{ticks:{callback:v=>v+'%'}}}}});

  const gEl=document.getElementById('charts-compare-grades');
  gEl.innerHTML=vers.map(v=>'<div class="chart-card"><div class="chart-title">'+v.toUpperCase()+' — ML Accuracy by Grade</div><div class="chart-container"><canvas id="cmp-g-'+v+'"></canvas></div></div>').join('');
  vers.forEach(v=>{const gr=stats[v].mlGrades,ord=['A','B','C','D'].filter(g=>gr[g]);CHARTS['cmp-g-'+v]=new Chart(document.getElementById('cmp-g-'+v),{type:'bar',data:{labels:ord.map(g=>'Grade '+g),datasets:[{label:'Accuracy',data:ord.map(g=>gr[g].total?((gr[g].correct/gr[g].total)*100).toFixed(1):0),backgroundColor:ord.map(g=>gCol(g)+'bb'),borderRadius:6},{label:'Count',data:ord.map(g=>gr[g].total),backgroundColor:'rgba(255,255,255,0.08)',borderRadius:6,yAxisID:'y1'}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,max:100,ticks:{callback:v=>v+'%'}},y1:{position:'right',beginAtZero:true,grid:{display:false},ticks:{color:'#555'}}}}});});

  const bEl=document.getElementById('charts-compare-bottom');
  bEl.innerHTML='<div class="chart-card full"><div class="chart-title">Cumulative PnL by League</div><div class="chart-container tall"><canvas id="cmp-lg"></canvas></div></div>';
  const allLg=new Set();vers.forEach(v=>Object.keys(stats[v].leagues).forEach(l=>allLg.add(l)));
  const lgArr=Array.from(allLg).map(l=>{let t=0;vers.forEach(v=>{if(stats[v].leagues[l])t+=stats[v].leagues[l].mlPnl;});return{name:l,t};}).sort((a,b)=>Math.abs(b.t)-Math.abs(a.t)).slice(0,12);
  CHARTS['cmp-lg']=new Chart(document.getElementById('cmp-lg'),{type:'bar',data:{labels:lgArr.map(l=>l.name.replace(/^(UEFA |England |Spain |Italy |France |Germany |Portugal |Netherlands |Mexico |USA )/,'')),datasets:vers.map(v=>({label:v.toUpperCase()+' ML PnL',data:lgArr.map(l=>stats[v].leagues[l.name]?stats[v].leagues[l.name].mlPnl.toFixed(2):0),backgroundColor:COLORS[v]+'99',borderRadius:4,barPercentage:0.7}))},options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',scales:{x:{ticks:{callback:v=>v+'u'}}},plugins:{legend:{position:'top'}}}});
}

function buildModel(ver,stats) {
  const ac=COLORS[ver],cl='kpi-'+ver;
  document.getElementById('kpi-'+ver).innerHTML=[
    '<div class="kpi '+cl+'"><div class="kpi-label">Settled</div><div class="kpi-value">'+stats.total+'</div><div class="kpi-sub">out of '+DATA[ver].length+'</div></div>',
    '<div class="kpi '+cl+'"><div class="kpi-label">ML Accuracy</div><div class="kpi-value">'+pct(stats.mlAcc)+'</div><div class="kpi-sub">'+stats.mlTotal+' bets</div></div>',
    '<div class="kpi '+cl+'"><div class="kpi-label">O/U Accuracy</div><div class="kpi-value">'+pct(stats.ouAcc)+'</div><div class="kpi-sub">'+stats.ouTotal+' bets</div></div>',
    '<div class="kpi '+cl+'"><div class="kpi-label">ML PnL</div><div class="kpi-value '+(stats.mlPnl>=0?'positive':'negative')+'">'+fmt(stats.mlPnl)+'</div><div class="kpi-sub">ROI: '+stats.mlROI.toFixed(1)+'%</div></div>',
    '<div class="kpi '+cl+'"><div class="kpi-label">O/U PnL</div><div class="kpi-value '+(stats.ouPnl>=0?'positive':'negative')+'">'+fmt(stats.ouPnl)+'</div><div class="kpi-sub">ROI: '+stats.ouROI.toFixed(1)+'%</div></div>'
  ].join('');

  document.getElementById('charts-'+ver+'-top').innerHTML='<div class="chart-card"><div class="chart-title">ML Accuracy by Grade</div><div class="chart-container"><canvas id="'+ver+'-mlg"></canvas></div></div><div class="chart-card"><div class="chart-title">O/U Accuracy by Grade</div><div class="chart-container"><canvas id="'+ver+'-oug"></canvas></div></div>';
  const mo=['A','B','C','D'].filter(g=>stats.mlGrades[g]);
  CHARTS[ver+'-mlg']=new Chart(document.getElementById(ver+'-mlg'),{type:'bar',data:{labels:mo.map(g=>'Grade '+g),datasets:[{data:mo.map(g=>stats.mlGrades[g].total?((stats.mlGrades[g].correct/stats.mlGrades[g].total)*100).toFixed(1):0),backgroundColor:mo.map(g=>gCol(g)+'bb'),borderRadius:8}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,max:100,ticks:{callback:v=>v+'%'}}}}});
  const oo=['A','B','C','D'].filter(g=>stats.ouGrades[g]);
  CHARTS[ver+'-oug']=new Chart(document.getElementById(ver+'-oug'),{type:'bar',data:{labels:oo.map(g=>'Grade '+g),datasets:[{data:oo.map(g=>stats.ouGrades[g].total?((stats.ouGrades[g].correct/stats.ouGrades[g].total)*100).toFixed(1):0),backgroundColor:oo.map(g=>gCol(g)+'bb'),borderRadius:8}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,max:100,ticks:{callback:v=>v+'%'}}}}});

  document.getElementById('charts-'+ver+'-bottom').innerHTML='<div class="chart-card"><div class="chart-title">PnL by Grade <span class="badge badge-ml">ML</span></div><div class="chart-container"><canvas id="'+ver+'-gpnl"></canvas></div></div><div class="chart-card"><div class="chart-title">Accuracy by Confidence</div><div class="chart-container"><canvas id="'+ver+'-conf"></canvas></div></div><div class="chart-card full"><div class="chart-title">League Performance</div><div class="chart-container tall"><canvas id="'+ver+'-lg"></canvas></div></div>';

  CHARTS[ver+'-gpnl']=new Chart(document.getElementById(ver+'-gpnl'),{type:'bar',data:{labels:mo.map(g=>'Grade '+g),datasets:[{data:mo.map(g=>stats.mlGrades[g].pnl.toFixed(2)),backgroundColor:mo.map(g=>stats.mlGrades[g].pnl>=0?COLORS.green+'aa':COLORS.red+'aa'),borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{ticks:{callback:v=>v+'u'}}}}});

  const cOrd=['VERY LOW','LOW','MEDIUM','HIGH','VERY HIGH'].filter(c=>stats.confLevels[c]);
  const cLbl=cOrd.length>1?cOrd:['LOW','MEDIUM','HIGH'];
  CHARTS[ver+'-conf']=new Chart(document.getElementById(ver+'-conf'),{type:'radar',data:{labels:cLbl,datasets:[{label:'ML',data:cLbl.map(c=>stats.confLevels[c]&&stats.confLevels[c].mlT?((stats.confLevels[c].mlC/stats.confLevels[c].mlT)*100).toFixed(1):0),borderColor:ac,backgroundColor:ac+'22',pointBackgroundColor:ac,borderWidth:2},{label:'O/U',data:cLbl.map(c=>stats.confLevels[c]&&stats.confLevels[c].ouT?((stats.confLevels[c].ouC/stats.confLevels[c].ouT)*100).toFixed(1):0),borderColor:'#8b5cf6',backgroundColor:'#8b5cf622',pointBackgroundColor:'#8b5cf6',borderWidth:2}]},options:{responsive:true,maintainAspectRatio:false,scales:{r:{beginAtZero:true,max:80,ticks:{display:false},grid:{color:'#1e2a3e'},angleLines:{color:'#1e2a3e'},pointLabels:{font:{size:10}}}}}});

  const le=Object.entries(stats.leagues).filter(([,v])=>v.mlT>=5).sort((a,b)=>(b[1].mlC/b[1].mlT)-(a[1].mlC/a[1].mlT)).slice(0,15);
  CHARTS[ver+'-lg']=new Chart(document.getElementById(ver+'-lg'),{type:'bar',data:{labels:le.map(([l])=>l.replace(/^(UEFA |England |Spain |Italy |France |Germany |Portugal |Netherlands |Mexico |USA )/,'')),datasets:[{label:'ML Acc %',data:le.map(([,v])=>((v.mlC/v.mlT)*100).toFixed(1)),backgroundColor:ac+'88',borderRadius:4,yAxisID:'y'},{label:'ML PnL',data:le.map(([,v])=>v.mlPnl.toFixed(2)),type:'line',borderColor:COLORS.green,backgroundColor:'transparent',pointBackgroundColor:le.map(([,v])=>v.mlPnl>=0?COLORS.green:COLORS.red),pointRadius:5,borderWidth:2,yAxisID:'y1',tension:0.3}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,max:100,ticks:{callback:v=>v+'%'}},y1:{position:'right',grid:{display:false}}}}});

  document.getElementById('table-'+ver).innerHTML='<div class="chart-title">Grade Breakdown</div><table class="data-table"><thead><tr><th>Grade</th><th>ML Acc</th><th>ML Bets</th><th>ML PnL</th><th>ML ROI</th><th>O/U Acc</th><th>O/U Bets</th><th>O/U PnL</th><th>O/U ROI</th></tr></thead><tbody>'+['A','B','C','D'].filter(g=>stats.mlGrades[g]||stats.ouGrades[g]).map(g=>{const ml=stats.mlGrades[g]||{correct:0,total:0,pnl:0},ou=stats.ouGrades[g]||{correct:0,total:0,pnl:0};return '<tr><td><span class="grade-pill grade-'+g+'">'+g+'</span></td><td>'+(ml.total?pct(ml.correct/ml.total):'—')+'</td><td>'+ml.total+'</td><td class="'+(ml.pnl>=0?'positive':'negative')+'">'+fmt(ml.pnl)+'</td><td>'+(ml.total?((ml.pnl/ml.total)*100).toFixed(1)+'%':'—')+'</td><td>'+(ou.total?pct(ou.correct/ou.total):'—')+'</td><td>'+ou.total+'</td><td class="'+(ou.pnl>=0?'positive':'negative')+'">'+fmt(ou.pnl)+'</td><td>'+(ou.total?((ou.pnl/ou.total)*100).toFixed(1)+'%':'—')+'</td></tr>';}).join('')+'</tbody></table>';
}

function switchTab(el) {
  const tab=el.dataset.tab;
  document.querySelectorAll('.tab').forEach(t=>t.className='tab');
  el.classList.add(tab==='compare'?'active-all':'active-'+tab);
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('panel-'+tab).classList.add('active');
  setTimeout(()=>{Object.values(CHARTS).forEach(c=>c.resize&&c.resize());},50);
}
</script>
</body>
</html>"""

    return html


# ──────────────────────────────────────────────────
# NBA DASHBOARD HTML GENERATOR
# ──────────────────────────────────────────────────
def generate_nba_html(b1_data):
    """Generate the NBA B1 dashboard HTML with data baked in."""

    b1_json = json.dumps(b1_data, default=json_serializer)

    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NBA Model B1 — Performance Analytics</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#06080d;--surface:#0d1117;--surface2:#151c28;--surface3:#1b2436;--border:#1c2538;--border-light:#253045;
  --text:#e4e9f1;--text-dim:#6b7a8d;--text-mid:#95a3b5;--accent:#e87523;--accent2:#ff9a50;
  --blue:#4a9eff;--green:#2dd4a0;--red:#f0576a;--yellow:#f5c542;--purple:#a78bfa;
  --grade-a:#2dd4a0;--grade-b:#4a9eff;--grade-c:#f5c542;--grade-d:#f0576a;
}
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:'Outfit',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;}
.header{padding:28px 36px 20px;display:flex;align-items:flex-end;gap:20px;border-bottom:1px solid var(--border);background:linear-gradient(180deg,#0a0f18 0%,var(--bg) 100%);}
.header-logo{width:44px;height:44px;background:linear-gradient(135deg,var(--accent),#ff6b2b);border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:800;color:#fff;flex-shrink:0;box-shadow:0 4px 20px rgba(232,117,35,0.25);}
.header-text h1{font-size:22px;font-weight:700;letter-spacing:-0.3px;color:#fff;} .header-text h1 span{color:var(--accent);}
.header-text p{color:var(--text-dim);font-size:13px;margin-top:2px;} .header-text .gen-time{color:var(--text-dim);font-size:11px;margin-top:3px;font-family:'IBM Plex Mono',monospace;}
.header-stats{margin-left:auto;display:flex;gap:24px;}
.header-stat{text-align:right;} .header-stat-label{font-size:10px;text-transform:uppercase;letter-spacing:1.2px;color:var(--text-dim);font-weight:600;} .header-stat-value{font-family:'IBM Plex Mono',monospace;font-size:18px;font-weight:600;margin-top:2px;}
.tabs{display:flex;gap:2px;padding:0 36px;background:var(--surface);border-bottom:1px solid var(--border);}
.tab{padding:12px 22px;font-size:12px;font-weight:600;color:var(--text-dim);cursor:pointer;border:none;background:none;font-family:'Outfit',sans-serif;letter-spacing:0.3px;position:relative;transition:color 0.2s;}
.tab:hover{color:var(--text-mid);} .tab.active{color:var(--accent);} .tab.active::after{content:'';position:absolute;bottom:0;left:12px;right:12px;height:2px;background:var(--accent);border-radius:2px 2px 0 0;}
.content{padding:24px 36px 60px;} .panel{display:none;} .panel.active{display:block;animation:fadeUp .35s ease;}
@keyframes fadeUp{from{opacity:0;transform:translateY(8px);}to{opacity:1;transform:translateY(0);}}
.kpi-row{display:grid;grid-template-columns:repeat(6,1fr);gap:12px;margin-bottom:24px;}
.kpi{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px 18px;position:relative;overflow:hidden;}
.kpi::after{content:'';position:absolute;top:0;left:0;width:100%;height:2px;}
.kpi.kpi-ml::after{background:var(--blue);} .kpi.kpi-ou::after{background:var(--green);} .kpi.kpi-sp::after{background:var(--purple);}
.kpi-label{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text-dim);font-weight:600;display:flex;align-items:center;gap:6px;}
.kpi-label .dot{width:6px;height:6px;border-radius:50%;} .dot-ml{background:var(--blue);} .dot-ou{background:var(--green);} .dot-sp{background:var(--purple);}
.kpi-value{font-family:'IBM Plex Mono',monospace;font-size:24px;font-weight:700;margin-top:6px;letter-spacing:-0.5px;}
.kpi-sub{font-size:11px;color:var(--text-dim);margin-top:3px;font-family:'IBM Plex Mono',monospace;}
.pos{color:var(--green);} .neg{color:var(--red);}
.chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px;} .chart-grid.tri{grid-template-columns:1fr 1fr 1fr;}
.chart-card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:20px 22px;} .chart-card.full{grid-column:1/-1;}
.chart-title{font-size:13px;font-weight:600;margin-bottom:14px;display:flex;align-items:center;gap:8px;}
.badge{font-size:9px;padding:2px 7px;border-radius:4px;font-weight:700;text-transform:uppercase;letter-spacing:0.5px;}
.badge-ml{background:rgba(74,158,255,0.12);color:var(--blue);} .badge-ou{background:rgba(45,212,160,0.12);color:var(--green);} .badge-sp{background:rgba(167,139,250,0.12);color:var(--purple);}
.chart-box{position:relative;height:270px;} .chart-box.tall{height:340px;} .chart-box.short{height:220px;}
.tbl{width:100%;border-collapse:collapse;font-size:12px;}
.tbl th{text-align:left;padding:8px 12px;font-size:10px;text-transform:uppercase;letter-spacing:0.8px;color:var(--text-dim);border-bottom:1px solid var(--border);font-weight:600;}
.tbl td{padding:9px 12px;border-bottom:1px solid rgba(28,37,56,0.5);font-family:'IBM Plex Mono',monospace;font-size:11px;}
.tbl tr:hover td{background:rgba(255,255,255,0.015);}
.gpill{display:inline-block;padding:2px 9px;border-radius:5px;font-weight:700;font-size:10px;}
.gA{background:rgba(45,212,160,0.12);color:var(--grade-a);} .gB{background:rgba(74,158,255,0.12);color:var(--grade-b);}
.gC{background:rgba(245,197,66,0.12);color:var(--grade-c);} .gD{background:rgba(240,87,106,0.12);color:var(--grade-d);}
.sec-title{font-size:16px;font-weight:700;margin:28px 0 16px;padding-bottom:10px;border-bottom:1px solid var(--border);} .sec-title span{color:var(--accent);}
@media(max-width:1100px){.chart-grid,.chart-grid.tri{grid-template-columns:1fr;}.kpi-row{grid-template-columns:repeat(3,1fr);}}
@media(max-width:700px){.kpi-row{grid-template-columns:repeat(2,1fr);}.content,.header,.tabs{padding-left:16px;padding-right:16px;}.header{flex-wrap:wrap;}.header-stats{margin-left:0;}}
</style>
</head>
<body>

<div class="header">
  <div class="header-logo">B1</div>
  <div class="header-text">
    <h1>NBA Model <span>B1</span> Analytics</h1>
    <p>Moneyline · Over/Under · Spread — Connected to PostgreSQL</p>
    <div class="gen-time">Generated: """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + r"""</div>
  </div>
  <div class="header-stats" id="headerStats"></div>
</div>

<div class="tabs" id="tabBar">
  <button class="tab active" data-tab="overview" onclick="switchTab(this)">Overview</button>
  <button class="tab" data-tab="grades" onclick="switchTab(this)">Grades & ROI</button>
  <button class="tab" data-tab="trends" onclick="switchTab(this)">Monthly Trends</button>
  <button class="tab" data-tab="confidence" onclick="switchTab(this)">Confidence</button>
  <button class="tab" data-tab="spreads" onclick="switchTab(this)">Spreads</button>
  <button class="tab" data-tab="teams" onclick="switchTab(this)">Teams</button>
</div>

<div class="content">
  <div id="dashboard">
    <div class="panel active" id="panel-overview"><div class="kpi-row" id="kpiOverview"></div><div class="chart-grid" id="overviewCharts"></div></div>
    <div class="panel" id="panel-grades"><div class="sec-title"><span>#1</span> Grade-wise Accuracy & ROI</div><div class="chart-grid tri" id="gradeAccCharts"></div><div class="chart-grid tri" id="gradeRoiCharts"></div><div class="chart-card" id="gradeTable" style="margin-top:16px"></div></div>
    <div class="panel" id="panel-trends"><div class="sec-title"><span>#2</span> Monthly Performance Trends</div><div class="chart-grid" id="trendCharts"></div><div class="chart-grid" id="trendCharts2"></div></div>
    <div class="panel" id="panel-confidence"><div class="sec-title"><span>#3</span> Confidence Level Analysis</div><div class="chart-grid" id="confCharts"></div><div class="chart-card" id="confTable" style="margin-top:16px"></div></div>
    <div class="panel" id="panel-spreads"><div class="sec-title"><span>#4</span> Spread Performance Breakdown</div><div class="kpi-row" id="kpiSpreads" style="grid-template-columns:repeat(4,1fr)"></div><div class="chart-grid" id="spreadCharts"></div></div>
    <div class="panel" id="panel-teams"><div class="sec-title">Team-level Accuracy & Point Prediction Error</div><div class="chart-grid" id="teamCharts"></div><div class="chart-card" id="teamTable" style="margin-top:16px"></div></div>
  </div>
</div>

<script>
// ─── DATA BAKED IN FROM POSTGRESQL ───
const RAW = """ + b1_json + r""";
const SETTLED = RAW.filter(r => r.status === 'SETTLED');

const C = {};
const COL = { blue:'#4a9eff',green:'#2dd4a0',red:'#f0576a',yellow:'#f5c542',purple:'#a78bfa',accent:'#e87523',gA:'#2dd4a0',gB:'#4a9eff',gC:'#f5c542',gD:'#f0576a' };

Chart.defaults.color='#6b7a8d'; Chart.defaults.borderColor='#1c2538'; Chart.defaults.font.family="'Outfit',sans-serif"; Chart.defaults.font.size=11;
Chart.defaults.plugins.legend.labels.boxWidth=10; Chart.defaults.plugins.legend.labels.padding=14;

const pct=n=>(n*100).toFixed(1)+'%';
const fmt=n=>(n>=0?'+':'')+n.toFixed(2);
const gCol=g=>({A:COL.gA,B:COL.gB,C:COL.gC,D:COL.gD}[g]||COL.gD);
const gCls=g=>({A:'gA',B:'gB',C:'gC',D:'gD'}[g]||'gD');

function mk(id,cfg){if(C[id])C[id].destroy();C[id]=new Chart(document.getElementById(id),cfg);}

function confBucket(val){if(val==null||val==='')return null;if(val>=80)return'VERY HIGH';if(val>=60)return'HIGH';if(val>=40)return'MEDIUM';if(val>=20)return'LOW';return'VERY LOW';}

// ─── BUILD ON LOAD ───
buildDashboard();

function buildDashboard(){
  const s=SETTLED;
  const mlValid=s.filter(r=>r.ml_correct!==null&&r.ml_correct!==''&&r.ml_correct!==undefined);
  const ouValid=s.filter(r=>r.ou_predicted&&r.ou_correct);
  const spValid=s.filter(r=>r.spread_covered_predicted!==null&&r.spread_covered_predicted!==''&&r.spread_covered_actual!==null&&r.spread_covered_actual!=='');
  const mlAcc=mlValid.length?mlValid.filter(r=>r.ml_correct===true||r.ml_correct===1).length/mlValid.length:0;
  const ouAcc=ouValid.length?ouValid.filter(r=>r.ou_predicted===r.ou_correct).length/ouValid.length:0;
  const spAcc=spValid.length?spValid.filter(r=>r.spread_covered_predicted===r.spread_covered_actual).length/spValid.length:0;
  const mlPnl=s.reduce((a,r)=>a+(r.ml_pnl||0),0);
  const ouPnl=s.reduce((a,r)=>a+(r.ou_pnl||0),0);
  const spPnl=s.reduce((a,r)=>a+(r.spread_pnl||0),0);

  document.getElementById('headerStats').innerHTML='<div class="header-stat"><div class="header-stat-label">Settled</div><div class="header-stat-value">'+s.length+'</div></div><div class="header-stat"><div class="header-stat-label">Total Rows</div><div class="header-stat-value">'+RAW.length+'</div></div>';

  buildOverview(s,mlAcc,ouAcc,spAcc,mlPnl,ouPnl,spPnl,mlValid,ouValid,spValid);
  buildGrades(s);
  buildTrends(s);
  buildConfidence(s);
  buildSpreads(s,spValid);
  buildTeams(s);
}

function buildOverview(s,mlAcc,ouAcc,spAcc,mlPnl,ouPnl,spPnl,mlV,ouV,spV){
  document.getElementById('kpiOverview').innerHTML=`
    <div class="kpi kpi-ml"><div class="kpi-label"><span class="dot dot-ml"></span>ML Accuracy</div><div class="kpi-value">${pct(mlAcc)}</div><div class="kpi-sub">${mlV.length} bets</div></div>
    <div class="kpi kpi-ou"><div class="kpi-label"><span class="dot dot-ou"></span>O/U Accuracy</div><div class="kpi-value">${pct(ouAcc)}</div><div class="kpi-sub">${ouV.length} bets</div></div>
    <div class="kpi kpi-sp"><div class="kpi-label"><span class="dot dot-sp"></span>Spread Accuracy</div><div class="kpi-value">${pct(spAcc)}</div><div class="kpi-sub">${spV.length} bets</div></div>
    <div class="kpi kpi-ml"><div class="kpi-label"><span class="dot dot-ml"></span>ML PnL</div><div class="kpi-value ${mlPnl>=0?'pos':'neg'}">${fmt(mlPnl)}</div><div class="kpi-sub">ROI: ${(mlV.length?(mlPnl/mlV.length)*100:0).toFixed(1)}%</div></div>
    <div class="kpi kpi-ou"><div class="kpi-label"><span class="dot dot-ou"></span>O/U PnL</div><div class="kpi-value ${ouPnl>=0?'pos':'neg'}">${fmt(ouPnl)}</div><div class="kpi-sub">ROI: ${(ouV.length?(ouPnl/ouV.length)*100:0).toFixed(1)}%</div></div>
    <div class="kpi kpi-sp"><div class="kpi-label"><span class="dot dot-sp"></span>Spread PnL</div><div class="kpi-value ${spPnl>=0?'pos':'neg'}">${fmt(spPnl)}</div><div class="kpi-sub">ROI: ${(spV.length?(spPnl/spV.length)*100:0).toFixed(1)}%</div></div>`;

  document.getElementById('overviewCharts').innerHTML=`
    <div class="chart-card"><div class="chart-title">Accuracy Comparison</div><div class="chart-box"><canvas id="ov-acc"></canvas></div></div>
    <div class="chart-card"><div class="chart-title">PnL & ROI</div><div class="chart-box"><canvas id="ov-pnl"></canvas></div></div>
    <div class="chart-card full"><div class="chart-title">Cumulative PnL Over Time</div><div class="chart-box"><canvas id="ov-cumul"></canvas></div></div>`;

  mk('ov-acc',{type:'bar',data:{labels:['Moneyline','Over/Under','Spread'],datasets:[{data:[mlAcc*100,ouAcc*100,spAcc*100],backgroundColor:[COL.blue+'bb',COL.green+'bb',COL.purple+'bb'],borderRadius:8,barPercentage:0.5}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,max:80,ticks:{callback:v=>v+'%'}}}}});
  mk('ov-pnl',{type:'bar',data:{labels:['ML PnL','ML ROI%','O/U PnL','O/U ROI%','Spr PnL','Spr ROI%'],datasets:[{data:[mlPnl,mlV.length?(mlPnl/mlV.length)*100:0,ouPnl,ouV.length?(ouPnl/ouV.length)*100:0,spPnl,spV.length?(spPnl/spV.length)*100:0],backgroundColor:d=>d.raw>=0?COL.green+'99':COL.red+'99',borderRadius:6,barPercentage:0.6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}}}});

  const sorted=[...s].sort((a,b)=>new Date(a.date)-new Date(b.date));
  let cumML=0,cumOU=0,cumSP=0;
  const cumData=sorted.map(r=>{cumML+=r.ml_pnl||0;cumOU+=r.ou_pnl||0;cumSP+=r.spread_pnl||0;return{ml:cumML,ou:cumOU,sp:cumSP,date:(r.date||'').substring(0,10)};});
  const step=Math.max(1,Math.floor(cumData.length/120));
  const sampled=cumData.filter((_,i)=>i%step===0||i===cumData.length-1);
  mk('ov-cumul',{type:'line',data:{labels:sampled.map(d=>d.date),datasets:[{label:'ML',data:sampled.map(d=>d.ml.toFixed(2)),borderColor:COL.blue,backgroundColor:'transparent',borderWidth:2,pointRadius:0,tension:0.3},{label:'O/U',data:sampled.map(d=>d.ou.toFixed(2)),borderColor:COL.green,backgroundColor:'transparent',borderWidth:2,pointRadius:0,tension:0.3},{label:'Spread',data:sampled.map(d=>d.sp.toFixed(2)),borderColor:COL.purple,backgroundColor:'transparent',borderWidth:2,pointRadius:0,tension:0.3}]},options:{responsive:true,maintainAspectRatio:false,scales:{x:{ticks:{maxTicksLimit:12}},y:{ticks:{callback:v=>v+'u'}}},plugins:{tooltip:{mode:'index',intersect:false}},interaction:{mode:'index',intersect:false}}});
}

function buildGrades(s){
  const grades=['A','B','C','D'];
  function gs(data,gc,cf,pc){const o={};grades.forEach(g=>{o[g]={c:0,t:0,pnl:0};});data.forEach(r=>{const g=r[gc];if(!g||!o[g])return;o[g].t++;if(cf(r))o[g].c++;o[g].pnl+=r[pc]||0;});return o;}
  const mlG=gs(s,'grade',r=>r.ml_correct===true||r.ml_correct===1,'ml_pnl');
  const ouG=gs(s,'ou_grade',r=>r.ou_predicted===r.ou_correct,'ou_pnl');
  const spG=gs(s,'spread_grade',r=>r.spread_covered_predicted===r.spread_covered_actual,'spread_pnl');
  document.getElementById('gradeAccCharts').innerHTML=`<div class="chart-card"><div class="chart-title">ML by Grade <span class="badge badge-ml">ML</span></div><div class="chart-box short"><canvas id="gr-ml-acc"></canvas></div></div><div class="chart-card"><div class="chart-title">O/U by Grade <span class="badge badge-ou">O/U</span></div><div class="chart-box short"><canvas id="gr-ou-acc"></canvas></div></div><div class="chart-card"><div class="chart-title">Spread by Grade <span class="badge badge-sp">SPR</span></div><div class="chart-box short"><canvas id="gr-sp-acc"></canvas></div></div>`;
  [['gr-ml-acc',mlG],['gr-ou-acc',ouG],['gr-sp-acc',spG]].forEach(([id,gd])=>{mk(id,{type:'bar',data:{labels:grades,datasets:[{label:'Accuracy',data:grades.map(g=>gd[g].t?((gd[g].c/gd[g].t)*100).toFixed(1):0),backgroundColor:grades.map(g=>gCol(g)+'bb'),borderRadius:6},{label:'Count',data:grades.map(g=>gd[g].t),backgroundColor:'rgba(255,255,255,0.06)',borderRadius:6,yAxisID:'y1'}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,max:100,ticks:{callback:v=>v+'%'}},y1:{position:'right',beginAtZero:true,grid:{display:false}}}}});});
  document.getElementById('gradeRoiCharts').innerHTML=`<div class="chart-card"><div class="chart-title">ML PnL by Grade</div><div class="chart-box short"><canvas id="gr-ml-pnl"></canvas></div></div><div class="chart-card"><div class="chart-title">O/U PnL by Grade</div><div class="chart-box short"><canvas id="gr-ou-pnl"></canvas></div></div><div class="chart-card"><div class="chart-title">Spread PnL by Grade</div><div class="chart-box short"><canvas id="gr-sp-pnl"></canvas></div></div>`;
  [['gr-ml-pnl',mlG],['gr-ou-pnl',ouG],['gr-sp-pnl',spG]].forEach(([id,gd])=>{mk(id,{type:'bar',data:{labels:grades,datasets:[{data:grades.map(g=>gd[g].pnl.toFixed(2)),backgroundColor:grades.map(g=>gd[g].pnl>=0?COL.green+'99':COL.red+'99'),borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{ticks:{callback:v=>v+'u'}}}}});});
  const tEl=document.getElementById('gradeTable');
  tEl.innerHTML='<div class="chart-title">Full Grade Breakdown</div><table class="tbl"><thead><tr><th>Grade</th><th>ML Acc</th><th>ML Bets</th><th>ML PnL</th><th>ML ROI</th><th>O/U Acc</th><th>O/U Bets</th><th>O/U PnL</th><th>Spr Acc</th><th>Spr Bets</th><th>Spr PnL</th></tr></thead><tbody>'+grades.map(g=>{const ml=mlG[g],ou=ouG[g],sp=spG[g];return '<tr><td><span class="gpill '+gCls(g)+'">'+g+'</span></td><td>'+(ml.t?pct(ml.c/ml.t):'—')+'</td><td>'+ml.t+'</td><td class="'+(ml.pnl>=0?'pos':'neg')+'">'+fmt(ml.pnl)+'</td><td>'+(ml.t?((ml.pnl/ml.t)*100).toFixed(1)+'%':'—')+'</td><td>'+(ou.t?pct(ou.c/ou.t):'—')+'</td><td>'+ou.t+'</td><td class="'+(ou.pnl>=0?'pos':'neg')+'">'+fmt(ou.pnl)+'</td><td>'+(sp.t?pct(sp.c/sp.t):'—')+'</td><td>'+sp.t+'</td><td class="'+(sp.pnl>=0?'pos':'neg')+'">'+fmt(sp.pnl)+'</td></tr>';}).join('')+'</tbody></table>';
}

function buildTrends(s){
  const months={};
  s.forEach(r=>{if(!r.date)return;const m=String(r.date).substring(0,7);if(!months[m])months[m]={mlC:0,mlT:0,ouC:0,ouT:0,spC:0,spT:0,mlPnl:0,ouPnl:0,spPnl:0};const mo=months[m];if(r.ml_correct!==null&&r.ml_correct!==''&&r.ml_correct!==undefined){mo.mlT++;if(r.ml_correct===true||r.ml_correct===1)mo.mlC++;}if(r.ou_predicted&&r.ou_correct){mo.ouT++;if(r.ou_predicted===r.ou_correct)mo.ouC++;}if(r.spread_covered_predicted!==null&&r.spread_covered_predicted!==''&&r.spread_covered_actual!==null&&r.spread_covered_actual!==''){mo.spT++;if(r.spread_covered_predicted===r.spread_covered_actual)mo.spC++;}mo.mlPnl+=r.ml_pnl||0;mo.ouPnl+=r.ou_pnl||0;mo.spPnl+=r.spread_pnl||0;});
  const keys=Object.keys(months).sort();
  const labels=keys.map(k=>{const[y,m]=k.split('-');return['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][+m-1]+' '+y.slice(2);});
  document.getElementById('trendCharts').innerHTML='<div class="chart-card"><div class="chart-title">Monthly Accuracy</div><div class="chart-box"><canvas id="tr-acc"></canvas></div></div><div class="chart-card"><div class="chart-title">Monthly PnL</div><div class="chart-box"><canvas id="tr-pnl"></canvas></div></div>';
  mk('tr-acc',{type:'line',data:{labels,datasets:[{label:'ML',data:keys.map(k=>months[k].mlT?((months[k].mlC/months[k].mlT)*100).toFixed(1):null),borderColor:COL.blue,backgroundColor:COL.blue+'22',fill:true,borderWidth:2.5,pointRadius:5,pointBackgroundColor:COL.blue,tension:0.3},{label:'O/U',data:keys.map(k=>months[k].ouT?((months[k].ouC/months[k].ouT)*100).toFixed(1):null),borderColor:COL.green,backgroundColor:COL.green+'22',fill:true,borderWidth:2.5,pointRadius:5,pointBackgroundColor:COL.green,tension:0.3},{label:'Spread',data:keys.map(k=>months[k].spT?((months[k].spC/months[k].spT)*100).toFixed(1):null),borderColor:COL.purple,backgroundColor:COL.purple+'22',fill:true,borderWidth:2.5,pointRadius:5,pointBackgroundColor:COL.purple,tension:0.3}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,max:80,ticks:{callback:v=>v+'%'}}}}});
  mk('tr-pnl',{type:'bar',data:{labels,datasets:[{label:'ML',data:keys.map(k=>months[k].mlPnl.toFixed(2)),backgroundColor:COL.blue+'88',borderRadius:4},{label:'O/U',data:keys.map(k=>months[k].ouPnl.toFixed(2)),backgroundColor:COL.green+'88',borderRadius:4},{label:'Spread',data:keys.map(k=>months[k].spPnl.toFixed(2)),backgroundColor:COL.purple+'88',borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{ticks:{callback:v=>v+'u'}}}}});
  document.getElementById('trendCharts2').innerHTML='<div class="chart-card"><div class="chart-title">Monthly Volume</div><div class="chart-box"><canvas id="tr-vol"></canvas></div></div><div class="chart-card"><div class="chart-title">Monthly ROI %</div><div class="chart-box"><canvas id="tr-roi"></canvas></div></div>';
  mk('tr-vol',{type:'bar',data:{labels,datasets:[{label:'ML',data:keys.map(k=>months[k].mlT),backgroundColor:COL.blue+'66',borderRadius:4,stack:'s'},{label:'O/U',data:keys.map(k=>months[k].ouT),backgroundColor:COL.green+'66',borderRadius:4,stack:'s'},{label:'Spread',data:keys.map(k=>months[k].spT),backgroundColor:COL.purple+'66',borderRadius:4,stack:'s'}]},options:{responsive:true,maintainAspectRatio:false}});
  mk('tr-roi',{type:'line',data:{labels,datasets:[{label:'ML ROI',data:keys.map(k=>months[k].mlT?((months[k].mlPnl/months[k].mlT)*100).toFixed(1):null),borderColor:COL.blue,borderWidth:2.5,pointRadius:5,pointBackgroundColor:COL.blue,tension:0.3,backgroundColor:'transparent'},{label:'O/U ROI',data:keys.map(k=>months[k].ouT?((months[k].ouPnl/months[k].ouT)*100).toFixed(1):null),borderColor:COL.green,borderWidth:2.5,pointRadius:5,pointBackgroundColor:COL.green,tension:0.3,backgroundColor:'transparent'},{label:'Spr ROI',data:keys.map(k=>months[k].spT?((months[k].spPnl/months[k].spT)*100).toFixed(1):null),borderColor:COL.purple,borderWidth:2.5,pointRadius:5,pointBackgroundColor:COL.purple,tension:0.3,backgroundColor:'transparent'}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{ticks:{callback:v=>v+'%'}}}}});
}

function buildConfidence(s){
  const buckets={};const order=['VERY LOW','LOW','MEDIUM','HIGH','VERY HIGH'];
  s.forEach(r=>{const b=confBucket(r.ml_confidence);if(!b)return;if(!buckets[b])buckets[b]={mlC:0,mlT:0,mlPnl:0,ouC:0,ouT:0,ouPnl:0};const bk=buckets[b];if(r.ml_correct!==null&&r.ml_correct!==''&&r.ml_correct!==undefined){bk.mlT++;if(r.ml_correct===true||r.ml_correct===1)bk.mlC++;bk.mlPnl+=r.ml_pnl||0;}if(r.ou_predicted&&r.ou_correct){bk.ouT++;if(r.ou_predicted===r.ou_correct)bk.ouC++;bk.ouPnl+=r.ou_pnl||0;}});
  const active=order.filter(b=>buckets[b]&&buckets[b].mlT>0);
  document.getElementById('confCharts').innerHTML='<div class="chart-card"><div class="chart-title">ML Accuracy by Confidence</div><div class="chart-box"><canvas id="cf-acc"></canvas></div></div><div class="chart-card"><div class="chart-title">ML PnL by Confidence</div><div class="chart-box"><canvas id="cf-pnl"></canvas></div></div><div class="chart-card full"><div class="chart-title">Confidence vs Win Rate</div><div class="chart-box"><canvas id="cf-scatter"></canvas></div></div>';
  mk('cf-acc',{type:'bar',data:{labels:active,datasets:[{data:active.map(b=>((buckets[b].mlC/buckets[b].mlT)*100).toFixed(1)),backgroundColor:active.map((_,i)=>`hsl(${20+i*40},80%,55%)`),borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{beginAtZero:true,max:85,ticks:{callback:v=>v+'%'}}}}});
  mk('cf-pnl',{type:'bar',data:{labels:active,datasets:[{data:active.map(b=>buckets[b].mlPnl.toFixed(2)),backgroundColor:active.map(b=>buckets[b].mlPnl>=0?COL.green+'99':COL.red+'99'),borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{y:{ticks:{callback:v=>v+'u'}}}}});
  const bins={};s.forEach(r=>{if(r.ml_confidence==null||r.ml_correct===null||r.ml_correct===''||r.ml_correct===undefined)return;const bin=Math.floor(r.ml_confidence/10)*10;if(!bins[bin])bins[bin]={x:bin+5,correct:0,total:0};bins[bin].total++;if(r.ml_correct===true||r.ml_correct===1)bins[bin].correct++;});
  mk('cf-scatter',{type:'bubble',data:{datasets:[{label:'Win Rate by Bin',data:Object.values(bins).map(b=>({x:b.x,y:b.total?((b.correct/b.total)*100):0,r:Math.max(4,Math.sqrt(b.total)*2.5)})),backgroundColor:COL.accent+'88',borderColor:COL.accent,borderWidth:1}]},options:{responsive:true,maintainAspectRatio:false,scales:{x:{title:{display:true,text:'Confidence %',color:'#6b7a8d'},min:0,max:105},y:{title:{display:true,text:'Win Rate %',color:'#6b7a8d'},min:0,max:100,ticks:{callback:v=>v+'%'}}}}});
  document.getElementById('confTable').innerHTML='<div class="chart-title">Confidence Breakdown</div><table class="tbl"><thead><tr><th>Bucket</th><th>ML Acc</th><th>ML Bets</th><th>ML PnL</th><th>ML ROI</th></tr></thead><tbody>'+active.map(b=>{const d=buckets[b];return '<tr><td style="font-weight:600">'+b+'</td><td>'+pct(d.mlC/d.mlT)+'</td><td>'+d.mlT+'</td><td class="'+(d.mlPnl>=0?'pos':'neg')+'">'+fmt(d.mlPnl)+'</td><td>'+((d.mlPnl/d.mlT)*100).toFixed(1)+'%</td></tr>';}).join('')+'</tbody></table>';
}

function buildSpreads(s,spV){
  const homeV=s.filter(r=>r.home_spread_covered_predicted!==null&&r.home_spread_covered_predicted!==''&&r.home_spread_covered_actual!==null&&r.home_spread_covered_actual!=='');
  const awayV=s.filter(r=>r.away_spread_covered_predicted!==null&&r.away_spread_covered_predicted!==''&&r.away_spread_covered_actual!==null&&r.away_spread_covered_actual!=='');
  const spAcc=spV.length?spV.filter(r=>r.spread_covered_predicted===r.spread_covered_actual).length/spV.length:0;
  const homeAcc=homeV.length?homeV.filter(r=>r.home_spread_covered_predicted===r.home_spread_covered_actual).length/homeV.length:0;
  const awayAcc=awayV.length?awayV.filter(r=>r.away_spread_covered_predicted===r.away_spread_covered_actual).length/awayV.length:0;
  const spPnl=s.reduce((a,r)=>a+(r.spread_pnl||0),0),homePnl=s.reduce((a,r)=>a+(r.home_spread_pnl||0),0),awayPnl=s.reduce((a,r)=>a+(r.away_spread_pnl||0),0);
  document.getElementById('kpiSpreads').innerHTML=`<div class="kpi kpi-sp"><div class="kpi-label"><span class="dot dot-sp"></span>Spread Acc</div><div class="kpi-value">${pct(spAcc)}</div><div class="kpi-sub">${spV.length} bets</div></div><div class="kpi kpi-sp"><div class="kpi-label">Home Spread</div><div class="kpi-value">${pct(homeAcc)}</div><div class="kpi-sub">${homeV.length} bets</div></div><div class="kpi kpi-sp"><div class="kpi-label">Away Spread</div><div class="kpi-value">${pct(awayAcc)}</div><div class="kpi-sub">${awayV.length} bets</div></div><div class="kpi kpi-sp"><div class="kpi-label">Away Spr PnL</div><div class="kpi-value ${awayPnl>=0?'pos':'neg'}">${fmt(awayPnl)}</div><div class="kpi-sub">Best performer</div></div>`;

  const grades=['A','B','C','D'],spGrade={};grades.forEach(g=>{spGrade[g]={c:0,t:0,pnl:0,hPnl:0,aPnl:0};});
  s.forEach(r=>{const g=r.spread_grade;if(!g||!spGrade[g])return;if(r.spread_covered_predicted!==null&&r.spread_covered_predicted!==''&&r.spread_covered_actual!==null&&r.spread_covered_actual!==''){spGrade[g].t++;if(r.spread_covered_predicted===r.spread_covered_actual)spGrade[g].c++;}spGrade[g].pnl+=r.spread_pnl||0;spGrade[g].hPnl+=r.home_spread_pnl||0;spGrade[g].aPnl+=r.away_spread_pnl||0;});
  document.getElementById('spreadCharts').innerHTML='<div class="chart-card"><div class="chart-title">Spread Acc by Grade</div><div class="chart-box"><canvas id="sp-grade"></canvas></div></div><div class="chart-card"><div class="chart-title">Home vs Away PnL by Grade</div><div class="chart-box"><canvas id="sp-ha-pnl"></canvas></div></div>';
  mk('sp-grade',{type:'bar',data:{labels:grades,datasets:[{label:'Accuracy',data:grades.map(g=>spGrade[g].t?((spGrade[g].c/spGrade[g].t)*100).toFixed(1):0),backgroundColor:grades.map(g=>gCol(g)+'bb'),borderRadius:6},{label:'Count',data:grades.map(g=>spGrade[g].t),backgroundColor:'rgba(255,255,255,0.06)',borderRadius:6,yAxisID:'y1'}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{beginAtZero:true,max:100,ticks:{callback:v=>v+'%'}},y1:{position:'right',beginAtZero:true,grid:{display:false}}}}});
  mk('sp-ha-pnl',{type:'bar',data:{labels:grades,datasets:[{label:'Home PnL',data:grades.map(g=>spGrade[g].hPnl.toFixed(2)),backgroundColor:COL.blue+'88',borderRadius:4},{label:'Away PnL',data:grades.map(g=>spGrade[g].aPnl.toFixed(2)),backgroundColor:COL.purple+'88',borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,scales:{y:{ticks:{callback:v=>v+'u'}}}}});
}

function buildTeams(s){
  const teams={};
  function ensure(t){if(!teams[t])teams[t]={games:0,mlC:0,mlT:0,mlPnl:0,ptErr:[]};}
  s.forEach(r=>{const ht=r.home_team,at=r.away_team;
    if(ht){ensure(ht);teams[ht].games++;if(r.ml_correct!==null&&r.ml_correct!==''&&r.ml_correct!==undefined){teams[ht].mlT++;if(r.ml_correct===true||r.ml_correct===1)teams[ht].mlC++;teams[ht].mlPnl+=r.ml_pnl||0;}if(r.home_points_predicted!=null&&r.home_points_actual!=null)teams[ht].ptErr.push(r.home_points_predicted-r.home_points_actual);}
    if(at){ensure(at);teams[at].games++;if(r.ml_correct!==null&&r.ml_correct!==''&&r.ml_correct!==undefined){teams[at].mlT++;if(r.ml_correct===true||r.ml_correct===1)teams[at].mlC++;teams[at].mlPnl+=r.ml_pnl||0;}if(r.away_points_predicted!=null&&r.away_points_actual!=null)teams[at].ptErr.push(r.away_points_predicted-r.away_points_actual);}
  });
  const sorted=Object.entries(teams).filter(([,v])=>v.mlT>=10).sort((a,b)=>b[1].mlT-a[1].mlT);
  const topByAcc=[...sorted].sort((a,b)=>(b[1].mlC/b[1].mlT)-(a[1].mlC/a[1].mlT)).slice(0,15);
  const topByErr=[...sorted].sort((a,b)=>{const ae=a[1].ptErr.length?Math.abs(a[1].ptErr.reduce((s,v)=>s+v,0)/a[1].ptErr.length):999;const be=b[1].ptErr.length?Math.abs(b[1].ptErr.reduce((s,v)=>s+v,0)/b[1].ptErr.length):999;return ae-be;}).slice(0,15);
  document.getElementById('teamCharts').innerHTML='<div class="chart-card"><div class="chart-title">Top 15 — ML Accuracy</div><div class="chart-box tall"><canvas id="tm-acc"></canvas></div></div><div class="chart-card"><div class="chart-title">Top 15 — Lowest Pt Error</div><div class="chart-box tall"><canvas id="tm-err"></canvas></div></div>';
  const sn=n=>n.replace(/(Golden State |Oklahoma City |Portland Trail |San Antonio |New Orleans |Minnesota |Los Angeles |Washington |Philadelphia |Milwaukee |Sacramento |Cleveland |Charlotte |Indiana |Brooklyn |Toronto |Orlando |Detroit |Phoenix |Chicago |Denver |Houston |Memphis |Boston |Dallas |Atlanta |Miami |Utah |New York )/,'').replace(' 76ers','76ers');
  mk('tm-acc',{type:'bar',data:{labels:topByAcc.map(([t])=>sn(t)),datasets:[{data:topByAcc.map(([,v])=>((v.mlC/v.mlT)*100).toFixed(1)),backgroundColor:topByAcc.map(([,v])=>{const a=v.mlC/v.mlT;return a>=0.7?COL.green+'99':a>=0.55?COL.blue+'99':COL.red+'77';}),borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',plugins:{legend:{display:false}},scales:{x:{beginAtZero:true,max:90,ticks:{callback:v=>v+'%'}}}}});
  mk('tm-err',{type:'bar',data:{labels:topByErr.map(([t])=>sn(t)),datasets:[{label:'Avg Abs Error',data:topByErr.map(([,v])=>v.ptErr.length?(v.ptErr.reduce((s,e)=>s+Math.abs(e),0)/v.ptErr.length).toFixed(1):0),backgroundColor:COL.accent+'88',borderRadius:4},{label:'Avg Bias',data:topByErr.map(([,v])=>v.ptErr.length?(v.ptErr.reduce((s,e)=>s+e,0)/v.ptErr.length).toFixed(1):0),backgroundColor:d=>d.raw>=0?COL.yellow+'77':COL.purple+'77',borderRadius:4}]},options:{responsive:true,maintainAspectRatio:false,indexAxis:'y',scales:{x:{title:{display:true,text:'Points',color:'#6b7a8d'}}}}});
  document.getElementById('teamTable').innerHTML='<div class="chart-title">All Teams (min 10 games)</div><table class="tbl"><thead><tr><th>Team</th><th>Games</th><th>ML Acc</th><th>ML PnL</th><th>ROI</th><th>Avg Pt Err</th><th>Bias</th></tr></thead><tbody>'+sorted.map(([t,v])=>{const acc=v.mlT?(v.mlC/v.mlT):0;const avgErr=v.ptErr.length?(v.ptErr.reduce((s,e)=>s+Math.abs(e),0)/v.ptErr.length):0;const avgBias=v.ptErr.length?(v.ptErr.reduce((s,e)=>s+e,0)/v.ptErr.length):0;const roi=v.mlT?((v.mlPnl/v.mlT)*100):0;return '<tr><td style="font-family:Outfit;font-weight:500">'+t+'</td><td>'+v.games+'</td><td>'+pct(acc)+'</td><td class="'+(v.mlPnl>=0?'pos':'neg')+'">'+fmt(v.mlPnl)+'</td><td class="'+(roi>=0?'pos':'neg')+'">'+roi.toFixed(1)+'%</td><td>'+avgErr.toFixed(1)+' pts</td><td class="'+(avgBias>=0?'pos':'neg')+'">'+(avgBias>=0?'+':'')+avgBias.toFixed(1)+' pts</td></tr>';}).join('')+'</tbody></table>';
}

function switchTab(el){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  el.classList.add('active');
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.getElementById('panel-'+el.dataset.tab).classList.add('active');
  setTimeout(()=>Object.values(C).forEach(c=>c.resize&&c.resize()),50);
}
</script>
</body>
</html>"""

    return html


# ──────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  Dashboard Generator — PostgreSQL → HTML")
    print("=" * 60)
    print()

    data = fetch_all_data()

    # Generate Soccer Dashboard
    print("Generating Soccer dashboard...")
    soccer_html = generate_soccer_html(
        data.get("soccer_v1", []),
        data.get("soccer_v2", []),
        data.get("soccer_v3", []),
    )
    with open(SOCCER_OUTPUT, "w", encoding="utf-8") as f:
        f.write(soccer_html)
    v1_settled = len([r for r in data.get("soccer_v1", []) if r.get("status") == "SETTLED"])
    v2_settled = len([r for r in data.get("soccer_v2", []) if r.get("status") == "SETTLED"])
    v3_settled = len([r for r in data.get("soccer_v3", []) if r.get("status") == "SETTLED"])
    print(f"  → {SOCCER_OUTPUT}")
    print(f"    V1: {len(data.get('soccer_v1', []))} rows ({v1_settled} settled)")
    print(f"    V2: {len(data.get('soccer_v2', []))} rows ({v2_settled} settled)")
    print(f"    V3: {len(data.get('soccer_v3', []))} rows ({v3_settled} settled)")
    print()

    # Generate NBA Dashboard
    print("Generating NBA dashboard...")
    nba_html = generate_nba_html(data.get("nba_b1", []))
    with open(NBA_OUTPUT, "w", encoding="utf-8") as f:
        f.write(nba_html)
    b1_settled = len([r for r in data.get("nba_b1", []) if r.get("status") == "SETTLED"])
    print(f"  → {NBA_OUTPUT}")
    print(f"    B1: {len(data.get('nba_b1', []))} rows ({b1_settled} settled)")
    print()

    print("Done! Open the HTML files in your browser.")
    print(f"  file://{SOCCER_OUTPUT}")
    print(f"  file://{NBA_OUTPUT}")


if __name__ == "__main__":
    main()
