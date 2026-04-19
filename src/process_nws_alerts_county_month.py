import pandas as pd
from pathlib import Path

IN_FILE = Path("data_processed") / "nws_alerts" / "alerts_county_day.csv"
OUT_DIR = Path("data_processed") / "nws_alerts"
OUT_DAY = OUT_DIR / "alerts_county_day_summary.csv"
OUT_MONTH = OUT_DIR / "alerts_county_month.csv"

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(IN_FILE, low_memory=False)

    # Keep only rows with county_fips present (zone-based alerts may have None here)
    df = df[df["county_fips"].notna()].copy()

    # Ensure date fields parse correctly
    df["snapshot_time_utc"] = pd.to_datetime(df["snapshot_time_utc"], errors="coerce", utc=True)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")  # YYYY-MM-DD
    df["MonthStart"] = df["snapshot_date"].dt.to_period("M").dt.to_timestamp()

    # === 1) COUNTY-DAY SUMMARY ===
    # For each county/day: count distinct active alert IDs
    day_summary = (
        df.groupby(["county_fips", "snapshot_date"], dropna=False)
          .agg(
              ActiveAlertsDistinct=("alert_id", "nunique"),
              AnySevere=("severity", lambda s: int((s == "Severe").any())),
              AnyExtreme=("severity", lambda s: int((s == "Extreme").any()))
          )
          .reset_index()
          .sort_values(["snapshot_date", "county_fips"])
    )
    day_summary.to_csv(OUT_DAY, index=False)

    # === 2) COUNTY-MONTH SUMMARY ===
    # A) Distinct alert IDs active at any time during the month (unique alerts)
    # B) AlertDaysActive: number of days with >=1 active alert in that county
    month_summary = (
        df.groupby(["county_fips", "MonthStart"], dropna=False)
          .agg(
              DistinctAlertsInMonth=("alert_id", "nunique"),
              AlertDaysActive=("snapshot_date", "nunique")
          )
          .reset_index()
          .sort_values(["MonthStart", "county_fips"])
    )

    # Add breakdown by severity (counts of distinct alert IDs by severity)
    severity_distinct = (
        df.dropna(subset=["severity"])
          .groupby(["county_fips", "MonthStart", "severity"])
          .agg(SeverityDistinctAlerts=("alert_id", "nunique"))
          .reset_index()
    )
    sev_pivot = severity_distinct.pivot_table(
        index=["county_fips", "MonthStart"],
        columns="severity",
        values="SeverityDistinctAlerts",
        fill_value=0,
        aggfunc="sum"
    ).reset_index()

    # Flatten pivot column names like Severe -> SevereDistinctAlerts
    sev_cols = []
    for c in sev_pivot.columns:
        if c in ["county_fips", "MonthStart"]:
            sev_cols.append(c)
        else:
            sev_cols.append(f"{c}DistinctAlerts")
    sev_pivot.columns = sev_cols

    # Merge severity pivot into month summary
    month_summary = month_summary.merge(sev_pivot, on=["county_fips", "MonthStart"], how="left")
    month_summary = month_summary.fillna(0)

    month_summary.to_csv(OUT_MONTH, index=False)

    print(f"Wrote: {OUT_DAY}  rows={len(day_summary)}")
    print(f"Wrote: {OUT_MONTH}  rows={len(month_summary)}")
    print("\nSample month rows:")
    print(month_summary.head(5).to_string(index=False))

if __name__ == "__main__":
    main()