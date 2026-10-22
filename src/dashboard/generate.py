import json
import os
import sys
import webbrowser

import snowflake.connector

WAREHOUSE = "earthquake_wh"
DATABASE = "earthquake_db"
SCHEMA = "gold"

QUERIES = {
    "summary": """
        SELECT
            COUNT(*) AS total_quakes,
            ROUND(AVG(magnitude), 2) AS avg_magnitude,
            MAX(magnitude) AS max_magnitude,
            COUNT(DISTINCT date_key) AS days_loaded,
            SUM(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) AS tsunami_count
        FROM fact_earthquake
    """,
    "by_band": """
        SELECT b.band_name, COUNT(*) AS count
        FROM fact_earthquake f
        JOIN dim_magnitude_band b ON f.magnitude_band_key = b.magnitude_band_key
        GROUP BY b.band_name, b.magnitude_band_key
        ORDER BY b.magnitude_band_key
    """,
    "by_region": """
        SELECT l.region, COUNT(*) AS count, ROUND(AVG(f.magnitude), 2) AS avg_mag
        FROM fact_earthquake f
        JOIN dim_location l ON f.location_key = l.location_key
        WHERE l.region IS NOT NULL
        GROUP BY l.region
        ORDER BY count DESC
        LIMIT 15
    """,
    "by_date": """
        SELECT d.full_date, COUNT(*) AS count, ROUND(AVG(f.magnitude), 2) AS avg_mag
        FROM fact_earthquake f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.full_date
        ORDER BY d.full_date
    """,
    "top_quakes": """
        SELECT f.magnitude, f.depth_km, l.place, l.region, f.event_time::VARCHAR AS event_time
        FROM fact_earthquake f
        JOIN dim_location l ON f.location_key = l.location_key
        ORDER BY f.magnitude DESC
        LIMIT 10
    """,
    "depth_vs_mag": """
        SELECT f.magnitude, f.depth_km
        FROM fact_earthquake f
        ORDER BY f.magnitude DESC
        LIMIT 500
    """,
}


def fetch_data(conn):
    data = {}
    cur = conn.cursor()
    for name, sql in QUERIES.items():
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        data[name] = rows
    cur.close()
    return data


def generate_html(data):
    summary = data["summary"][0]

    top_rows = ""
    for q in data["top_quakes"]:
        top_rows += f"""<tr>
            <td>{q['magnitude']}</td>
            <td>{q['depth_km']} km</td>
            <td>{q['place']}</td>
            <td>{q['event_time'][:19]}</td>
        </tr>"""

    by_band_labels = json.dumps([r["band_name"] for r in data["by_band"]])
    by_band_values = json.dumps([r["count"] for r in data["by_band"]])
    by_region_labels = json.dumps([r["region"] for r in data["by_region"]])
    by_region_values = json.dumps([r["count"] for r in data["by_region"]])
    by_date_labels = json.dumps([str(r["full_date"]) for r in data["by_date"]])
    by_date_counts = json.dumps([r["count"] for r in data["by_date"]])
    by_date_avg = json.dumps([float(r["avg_mag"]) for r in data["by_date"]])
    scatter_data = json.dumps(
        [{"x": float(r["magnitude"]), "y": float(r["depth_km"])} for r in data["depth_vs_mag"]]
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Earthquake Pipeline Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root {{
    --bg: #0f172a; --card: #1e293b; --border: #334155;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #38bdf8;
    --red: #ef4444; --orange: #f97316; --yellow: #eab308;
    --green: #22c55e; --blue: #3b82f6; --purple: #a855f7;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); padding: 16px; }}
.header {{ text-align: center; margin-bottom: 24px; }}
.header h1 {{ font-size: 1.8rem; margin-bottom: 4px; }}
.header p {{ color: var(--muted); font-size: 0.9rem; }}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-bottom: 24px; }}
.stat {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; text-align: center; }}
.stat .value {{ font-size: 1.8rem; font-weight: 700; color: var(--accent); }}
.stat .label {{ font-size: 0.78rem; color: var(--muted); margin-top: 4px; text-transform: uppercase; letter-spacing: 1px; }}
.grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 16px; margin-bottom: 24px; }}
.card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }}
.card h2 {{ font-size: 1rem; color: var(--muted); margin-bottom: 14px; font-weight: 500; }}
.card.full {{ grid-column: 1 / -1; }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.88rem; }}
th {{ text-align: left; color: var(--muted); font-weight: 500; padding: 8px; border-bottom: 1px solid var(--border); }}
td {{ padding: 8px; border-bottom: 1px solid var(--border); }}
tr:hover td {{ background: rgba(56,189,248,0.06); }}
canvas {{ max-height: 300px; }}
.footer {{ text-align: center; color: var(--muted); font-size: 0.78rem; margin-top: 16px; }}
</style>
</head>
<body>
<div class="header">
    <h1>Global Seismic Activity Dashboard</h1>
    <p>Data pipeline: USGS API &rarr; S3 Bronze &rarr; PySpark Silver &rarr; Snowflake Gold</p>
</div>

<div class="stats">
    <div class="stat"><div class="value">{summary['total_quakes']}</div><div class="label">Total Earthquakes</div></div>
    <div class="stat"><div class="value">{summary['avg_magnitude']}</div><div class="label">Avg Magnitude</div></div>
    <div class="stat"><div class="value">{summary['max_magnitude']}</div><div class="label">Max Magnitude</div></div>
    <div class="stat"><div class="value">{summary['days_loaded']}</div><div class="label">Days Loaded</div></div>
    <div class="stat"><div class="value">{summary['tsunami_count']}</div><div class="label">Tsunami Alerts</div></div>
</div>

<div class="grid">
    <div class="card">
        <h2>Earthquakes by Magnitude Band</h2>
        <canvas id="bandChart"></canvas>
    </div>
    <div class="card">
        <h2>Top 15 Regions by Count</h2>
        <canvas id="regionChart"></canvas>
    </div>
    <div class="card full">
        <h2>Daily Earthquake Count &amp; Average Magnitude</h2>
        <canvas id="dateChart" style="max-height:250px"></canvas>
    </div>
    <div class="card">
        <h2>Depth vs Magnitude (top 500)</h2>
        <canvas id="scatterChart"></canvas>
    </div>
    <div class="card">
        <h2>Top 10 Strongest Earthquakes</h2>
        <table>
            <thead><tr><th>Mag</th><th>Depth</th><th>Location</th><th>Time</th></tr></thead>
            <tbody>{top_rows}</tbody>
        </table>
    </div>
</div>

<div class="footer">Generated from earthquake_db.gold &middot; Snowflake &middot; earthquake-etl-pipeline</div>

<script>
const colors = ['#38bdf8','#22c55e','#eab308','#f97316','#ef4444','#a855f7','#ec4899'];

new Chart(document.getElementById('bandChart'), {{
    type: 'doughnut',
    data: {{
        labels: {by_band_labels},
        datasets: [{{ data: {by_band_values}, backgroundColor: colors, borderColor: '#1e293b', borderWidth: 2 }}]
    }},
    options: {{ responsive: true, plugins: {{ legend: {{ position: 'right', labels: {{ color: '#94a3b8' }} }} }} }}
}});

new Chart(document.getElementById('regionChart'), {{
    type: 'bar',
    data: {{
        labels: {by_region_labels},
        datasets: [{{ label: 'Count', data: {by_region_values}, backgroundColor: '#3b82f6' }}]
    }},
    options: {{
        indexAxis: 'y', responsive: true,
        scales: {{ x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }}, y: {{ ticks: {{ color: '#94a3b8', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }},
        plugins: {{ legend: {{ display: false }} }}
    }}
}});

new Chart(document.getElementById('dateChart'), {{
    type: 'bar',
    data: {{
        labels: {by_date_labels},
        datasets: [
            {{ label: 'Count', data: {by_date_counts}, backgroundColor: 'rgba(59,130,246,0.5)', borderColor: '#3b82f6', borderWidth: 1, yAxisID: 'y', order: 2 }},
            {{ label: 'Avg Magnitude', data: {by_date_avg}, type: 'line', borderColor: '#f97316', backgroundColor: 'transparent', borderWidth: 2, pointRadius: 2, yAxisID: 'y1', order: 1 }}
        ]
    }},
    options: {{
        responsive: true,
        scales: {{
            x: {{ ticks: {{ color: '#94a3b8', maxRotation: 45 }}, grid: {{ display: false }} }},
            y: {{ position: 'left', ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Count', color: '#94a3b8' }} }},
            y1: {{ position: 'right', ticks: {{ color: '#f97316' }}, grid: {{ display: false }}, title: {{ display: true, text: 'Avg Mag', color: '#f97316' }}, min: 2, max: 6 }}
        }},
        plugins: {{ legend: {{ labels: {{ color: '#94a3b8' }} }} }}
    }}
}});

new Chart(document.getElementById('scatterChart'), {{
    type: 'scatter',
    data: {{
        datasets: [{{ label: 'Earthquakes', data: {scatter_data}, backgroundColor: 'rgba(168,85,247,0.5)', pointRadius: 3 }}]
    }},
    options: {{
        responsive: true,
        scales: {{
            x: {{ title: {{ display: true, text: 'Magnitude', color: '#94a3b8' }}, ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }},
            y: {{ title: {{ display: true, text: 'Depth (km)', color: '#94a3b8' }}, ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }} }}
        }},
        plugins: {{ legend: {{ display: false }} }}
    }}
}});
</script>
</body>
</html>"""


def main():
    conn = snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
    )

    try:
        data = fetch_data(conn)
    finally:
        conn.close()

    html = generate_html(data)
    output = os.path.join(os.path.dirname(__file__), "..", "..", "dashboard.html")
    output = os.path.abspath(output)

    with open(output, "w") as f:
        f.write(html)

    print(f"Dashboard generated: {output}")
    webbrowser.open(f"file://{output}")


if __name__ == "__main__":
    main()
