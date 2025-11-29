import argparse
import os

import pandas as pd
from sqlalchemy import create_engine, text
import matplotlib.pyplot as plt
import seaborn as sns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate heatmap of weekly anomalies for corporate repositories."
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
        help="First ISO year to include (default: 2005)",
    )

    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help="Last ISO year to include (default: 2025)",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default="./plots",
        help="Directory to save the heatmap PNG files (default: ./plots)",
    )

    return parser.parse_args()


def load_anomaly_data(engine, start_year: int, end_year: int) -> pd.DataFrame:
    query = text(
        """
        SELECT
            year AS iso_year,
            iso_week,
            COUNT(DISTINCT repo_id) AS repos_with_anomalies
        FROM repository_weekly_activity_anomalies
        WHERE year BETWEEN :start_year AND :end_year
        GROUP BY year, iso_week
        ORDER BY year, iso_week
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"start_year": start_year, "end_year": end_year})

    print(f"[activity_heatmap] Loaded {len(df)} anomaly rows.")
    return df


def make_heatmap(df: pd.DataFrame, output_path: str) -> None:
    """
    Build and save heatmap from anomaly dataframe.

    df: columns [iso_year, iso_week, repos_with_anomalies]
    """
    if df.empty:
        print("[activity_heatmap] No data to plot. Exiting.")
        return

    # Pivot: rows = years, columns = weeks, values = number of repos with anomalies
    table = df.pivot(
        index="iso_year",
        columns="iso_week",
        values="repos_with_anomalies",
    ).fillna(0)

    # Ensure weeks are in order 1..53
    table = table.reindex(sorted(table.columns), axis=1)

    plt.figure(figsize=(18, 8))
    sns.heatmap(
        table,
        cmap="viridis",
        linewidths=0.3,
        linecolor="gray",
        cbar_kws={"label": "Repos with anomalies"},
    )

    plt.title("Weekly anomaly activity (number of repos with anomalies)")
    plt.xlabel("ISO week")
    plt.ylabel("ISO year")

    # Make ticks a bit nicer
    plt.xticks(rotation=0)
    plt.yticks(rotation=0)

    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()

    print(f"[activity_heatmap] Saved heatmap to {output_path}")


def main():
    args = parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print("[activity_heatmap] Connecting to database...")
    engine = create_engine(args.database_url)

    print(
        f"[activity_heatmap] Loading anomaly data for years "
        f"{args.start_year}–{args.end_year}..."
    )
    df = load_anomaly_data(engine, args.start_year, args.end_year)

    output_path = os.path.join(
        args.output_dir,
        f"weekly_anomalies_heatmap_{args.start_year}_{args.end_year}.png",
    )
    make_heatmap(df, output_path)


if __name__ == "__main__":
    main()
