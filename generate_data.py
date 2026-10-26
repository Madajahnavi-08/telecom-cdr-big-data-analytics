from pathlib import Path
import csv, random
from datetime import datetime, timedelta

random.seed(17)
out = Path(__file__).parent / "sample_data" / "cdr_sample.csv"
out.parent.mkdir(parents=True, exist_ok=True)
sources = ["CRM_GATEWAY", "BILLING_SYSTEM", "NETWORK_SWITCH"]
statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "DROPPED", "FAILED"]
start = datetime(2026, 10, 20, 0, 0, 0)
rows = []
for i in range(600):
    ts = start + timedelta(minutes=random.randint(0, 3 * 24 * 60))
    rows.append({
        "timestamp": ts.isoformat(sep=" "),
        "source": random.choice(sources),
        "caller_id": f"SUB{random.randint(1000, 1099)}",
        "receiver_id": f"SUB{random.randint(1100, 1199)}",
        "duration_sec": random.randint(0, 900),
        "data_mb": round(random.uniform(0, 120), 2),
        "cell_id": f"CELL_{random.randint(1, 25):02d}",
        "call_status": random.choice(statuses),
    })
with out.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
print(f"Created {len(rows)} sample records at {out}")
