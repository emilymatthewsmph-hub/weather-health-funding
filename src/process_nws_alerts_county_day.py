import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SNAP_DIR = Path("data_raw") / "nws_alerts" / "snapshots"
OUT_DIR = Path("data_processed") / "nws_alerts"
OUT_FILE = OUT_DIR / "alerts_county_day.csv"


def latest_snapshot_file():
    files = sorted(SNAP_DIR.glob("alerts_active_TX_*.json"))
    if not files:
        raise FileNotFoundError(f"No snapshot files found in {SNAP_DIR}")
    return files[-1]


def parse_snapshot_time_from_filename(path: Path):
    # alerts_active_TX_YYYYMMDDTHHMMSSZ.json
    m = re.search(r"alerts_active_TX_(\d{8}T\d{6}Z)\.json$", path.name)
    if not m:
        return datetime.now(timezone.utc)
    return datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def normalize_same_to_county_fips(same_code: str):
    """
    SAME codes are usually 6 digits: 0 + stateFIPS(2) + countyFIPS(3)
    Example: '048027' -> county FIPS '48027'
    Sometimes there are other lengths; we handle the common case and fall back safely.
    """
    if same_code is None:
        return None
    s = str(same_code).strip()
    if len(s) == 6 and s.isdigit() and s.startswith("0"):
        return s[1:]  # drop the leading 0
    # Some alerts may provide just county fips or other forms; keep digits if 5-length
    if len(s) == 5 and s.isdigit():
        return s
    return None


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    snap_file = latest_snapshot_file()
    snapshot_time = parse_snapshot_time_from_filename(snap_file)

    print(f"Using snapshot: {snap_file.name}")
    print(f"Snapshot time (UTC): {snapshot_time.isoformat()}")

    data = json.loads(snap_file.read_text(encoding="utf-8"))
    features = data.get("features", [])

    rows = []
    for feat in features:
        props = feat.get("properties", {})

        alert_id = props.get("id") or feat.get("id")
        event = props.get("event")
        headline = props.get("headline")
        severity = props.get("severity")
        urgency = props.get("urgency")
        certainty = props.get("certainty")
        status = props.get("status")
        msg_type = props.get("messageType")

        sent = props.get("sent")
        effective = props.get("effective")
        onset = props.get("onset")
        expires = props.get("expires")
        ends = props.get("ends")

        # County mapping via SAME codes (preferred for county drill-down)
        same_list = safe_get(props, "geocode", "SAME", default=[])
        if same_list is None:
            same_list = []

        county_fips_list = []
        for same_code in same_list:
            fips = normalize_same_to_county_fips(same_code)
            if fips:
                county_fips_list.append(fips)

        # If we don't have SAME codes, we keep one row with county_fips = None
        # (later we can handle zone-based alerts separately if needed)
        if not county_fips_list:
            county_fips_list = [None]

        for county_fips in county_fips_list:
            rows.append(
                {
                    "snapshot_time_utc": snapshot_time.isoformat(),
                    "snapshot_date": snapshot_time.date().isoformat(),
                    "alert_id": alert_id,
                    "event": event,
                    "headline": headline,
                    "severity": severity,
                    "urgency": urgency,
                    "certainty": certainty,
                    "status": status,
                    "message_type": msg_type,
                    "sent": sent,
                    "effective": effective,
                    "onset": onset,
                    "expires": expires,
                    "ends": ends,
                    "county_fips": county_fips,
                }
            )

    df = pd.DataFrame(rows)

    # Make a "day" column to support county-day grouping
    df["alert_day_utc"] = pd.to_datetime(df["snapshot_date"], errors="coerce")

    df.to_csv(OUT_FILE, index=False)
    print(f"\nWrote: {OUT_FILE}")
    print(f"Rows: {len(df)}")
    print("Sample rows:")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()