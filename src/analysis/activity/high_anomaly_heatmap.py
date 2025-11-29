import argparse
import os

import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate heatmap of HIGH anomalies only."
    )

    parser.add_argument(
        "--database-url",
        default="postgresql://postgres:password@localhost/github_analysis",
        help="PostgreSQL database URL",
    )

    parser.add_argument(
        "--start-year",
        type=int,
        default=2005,
        help="First ISO year to include",
    )

    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="Last ISO year to include",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="./plots",
        help="Directory to save the heatmap PNG",
    )

    return parser.parse_args()


def load_high_anomaly_data(engine, start_year: int, end_year: int) -> pd.DataFrame:
    query = text(
        """
        SELECT
            year AS iso_year,
            iso_week,
            COUNT(DISTINCT repo_id) AS repos_with_high_anomalies
        FROM repository_weekly_activity_anomalies
        WHERE direction = 'high'
          AND year BETWEEN :start_year AND :end_year
        GROUP BY year, iso_week
        ORDER BY year, iso_week
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start_year": start_year, "end_year": end_year})

    print(f"[high_anomaly_heatmap] Loaded {len(df)} HIGH anomaly rows.")
    return df


def make_heatmap(df: pd.DataFrame, output_path: str) -> None:
    if df.empty:
        print("[high_anomaly_heatmap] No data to plot.")
        return

    table = df.pivot(
        index="iso_year",
        columns="iso_week",
        values="repos_with_high_anomalies",
    ).fillna(0)

    table = table.reindex(sorted(table.columns), axis=1)

    plt.figure(figsize=(18, 8))
    sns.heatmap(
        table,
        cmap="YlGnBu",    # nice for highlighting “bursts”
        linewidths=0.3,
        linecolor="gray",
        cbar_kws={"label": "Repos with HIGH anomalies"},
    )

    plt.title("Weekly HIGH anomalies (number of repos with high anomalies)")
    plt.xlabel("ISO week")
    plt.ylabel("ISO year")
    plt.xticks(rotation=0)
    plt.yticks(rotation=0)

    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()

    print(f"[high_anomaly_heatmap] Saved heatmap to {output_path}")


def main():
    args = parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print("[high_anomaly_heatmap] Connecting to database…")
    engine = create_engine(args.database_url)

    print(f"[high_anomaly_heatmap] Loading HIGH anomalies for {args.start_year}–{args.end_year}…")
    df = load_high_anomaly_data(engine, args.start_year, args.end_year)

    output_path = os.path.join(
        args.output_dir,
        f"high_anomalies_heatmap_{args.start_year}_{args.end_year}.png",
    )

    make_heatmap(df, output_path)


if __name__ == "__main__":
    main()
