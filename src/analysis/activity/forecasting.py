#!/usr/bin/env python3
"""
Daily activity anomaly detection using DOW-aware MAD (v3), with optional multiprocessing.

For each repository and each day:

1. Build a daily time series of commit counts (missing days → 0).
2. For each day t with weekday d, look back over previous N weeks and
   collect days with the same weekday d.
3. In log1p space, compute a robust baseline (median) and variability (MAD).
4. Forecast expected commits for day t as baseline_commits = exp(median) - 1.
5. Compute modified z-score using MAD and flag spikes/drops as anomalies.

This detects:
- holiday drops (e.g., Christmas, New Year),
- hackathon spikes,
- other unusual day-level events,
relative to recent behavior for that weekday.
"""

import os
import multiprocessing as mp
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd

from src.analysis.activity.repo.repo import ActivityRepository
from src.storage.unit_of_work import UnitOfWork


# ----------------- worker helper -----------------


def _analyze_repo_worker(args: Tuple) -> pd.DataFrame or None:
    """
    Standalone worker function for multiprocessing.

    args must contain:
      (repo_id, g_df, lookback_weeks, min_history_points, min_window_commits,
       min_total_commits, min_active_days, mad_threshold, min_abs_diff)
    """
    (
        repo_id,
        g,
        lookback_weeks,
        min_history_points,
        min_window_commits,
        min_total_commits,
        min_active_days,
        mad_threshold,
        min_abs_diff,
    ) = args

    # g: DataFrame with index = DatetimeIndex (daily), columns: ['repo_id', 'commit_count']
    total_commits = int(g["commit_count"].sum())
    active_days = int((g["commit_count"] > 0).sum())

    if total_commits < min_total_commits or active_days < min_active_days:
        return None

    dow = g.index.dayofweek
    dates = g.index.to_list()
    counts = g["commit_count"].to_numpy(dtype=float)

    predicted_list: List[float or None] = []
    residual_list: List[float or None] = []
    z_list: List[float or None] = []
    anomaly_list: List[bool] = []

    lookback_days = lookback_weeks * 7

    for i, current_date in enumerate(dates):
        current_count = counts[i]
        current_dow = dow[i]

        window_start = current_date - pd.Timedelta(days=lookback_days)
        mask = (
            (g.index < current_date)
            & (g.index >= window_start)
            & (dow == current_dow)
        )

        history_counts = g.loc[mask, "commit_count"].to_numpy(dtype=float)

        baseline_commits = None
        z_score = None
        residual = None
        is_anomaly = False

        if (
            len(history_counts) >= min_history_points
            and history_counts.sum() >= min_window_commits
        ):
            # Robust baseline in log1p space
            x_hist = np.log1p(history_counts)
            median_x = float(np.median(x_hist))
            mad = float(np.median(np.abs(x_hist - median_x)))

            baseline_commits = float(np.expm1(median_x))

            x_today = float(np.log1p(current_count))

            if mad > 1e-9:
                z = 0.6745 * (x_today - median_x) / mad
                z_score = float(z)
                residual = float(current_count - baseline_commits)

                if (
                    abs(z_score) >= mad_threshold
                    and abs(residual) >= min_abs_diff
                ):
                    is_anomaly = True
            else:
                residual = float(current_count - baseline_commits)
        else:
            # Not enough history → baseline = current value, no anomaly
            baseline_commits = float(current_count)
            residual = 0.0
            z_score = None
            is_anomaly = False

        predicted_list.append(baseline_commits)
        residual_list.append(residual)
        z_list.append(z_score)
        anomaly_list.append(is_anomaly)

    out = pd.DataFrame(
        {
            "repo_id": int(repo_id),
            "activity_date": dates,
            "actual": counts.astype(int),
            "predicted": predicted_list,
            "residual": residual_list,
            "z_score": z_list,
            "is_anomaly": anomaly_list,
        }
    )
    return out


# ----------------- core class -----------------


class DailyMADActivityCore:
    """
    Core daily anomaly detector using DOW+MAD, with optional multiprocessing.

    Parameters:
      lookback_weeks: how many weeks back to use for baseline (per DOW)
      min_history_points: min number of same-DOW days in window to compute baseline
      min_window_commits: min total commits in history window to trust baseline
      min_total_commits: min total commits per repo to analyze it at all
      min_active_days: min days with commits > 0 per repo
      mad_threshold: |modified_z| above this is considered anomalous
      min_abs_diff: minimum absolute difference (actual - baseline) to count as anomaly
      n_workers: number of worker processes (None or 1 = no multiprocessing)
    """

    def __init__(
        self,
        repo: ActivityRepository,
        lookback_weeks: int = 8,
        min_history_points: int = 4,
        min_window_commits: int = 5,
        min_total_commits: int = 30,
        min_active_days: int = 10,
        mad_threshold: float = 3.5,
        min_abs_diff: int = 5,
        n_workers: int or None = None,
    ):
        self.repo = repo
        self.lookback_weeks = lookback_weeks
        self.min_history_points = min_history_points
        self.min_window_commits = min_window_commits
        self.min_total_commits = min_total_commits
        self.min_active_days = min_active_days
        self.mad_threshold = mad_threshold
        self.min_abs_diff = min_abs_diff
        self.n_workers = n_workers

    # ----------------- public API -----------------

    def run(self) -> pd.DataFrame or Dict[str, Any]:
        df = self.repo.load_daily_commit_data()
        if df.empty:
            print("[DailyMADActivityCore] No daily activity data found.")
            return {"error": "No daily activity data"}

        df["activity_date"] = pd.to_datetime(df["activity_date"])

        # Prepare per-repo jobs
        jobs: List[Tuple] = []
        for repo_id, g in df.groupby("repo_id"):
            g = g.sort_values("activity_date")
            g = g.set_index("activity_date").asfreq("D", fill_value=0)
            g["repo_id"] = repo_id
            g = g[["repo_id", "commit_count"]]

            jobs.append(
                (
                    int(repo_id),
                    g,
                    self.lookback_weeks,
                    self.min_history_points,
                    self.min_window_commits,
                    self.min_total_commits,
                    self.min_active_days,
                    self.mad_threshold,
                    self.min_abs_diff,
                )
            )

        results: List[pd.DataFrame] = []

        if self.n_workers is not None and self.n_workers > 1:
            print(f"[DailyMADActivityCore] Running with {self.n_workers} workers...")
            with mp.Pool(self.n_workers) as pool:
                for out in pool.imap_unordered(_analyze_repo_worker, jobs, chunksize=10):
                    if out is not None and not out.empty:
                        results.append(out)
        else:
            print("[DailyMADActivityCore] Running single-threaded...")
            for job in jobs:
                out = _analyze_repo_worker(job)
                if out is not None and not out.empty:
                    results.append(out)

        if not results:
            print("[DailyMADActivityCore] No repos qualified for analysis.")
            return {"error": "No qualified repos"}

        result_df = pd.concat(results, ignore_index=True)

        os.makedirs("artifacts", exist_ok=True)
        result_df.to_csv("artifacts/daily_mad_activity.csv", index=False)

        return result_df


# ----------------- analyzer wrapper -----------------


class DailyMADActivityAnalyzer:
    """
    High-level API used by main.py.

    Example:
        activity_analyzer = HoltWintersActivityAnalyzer(database_url, n_workers=6)
        results = activity_analyzer.analyze()
    """

    def __init__(self, database_url: str, n_workers: int or None = None):
        self.database_url = database_url
        self.repo = ActivityRepository(database_url)
        self.core = DailyMADActivityCore(self.repo, n_workers=n_workers)

        uow = UnitOfWork(database_url)
        uow.create_activity_tables()

    def analyze(self) -> Dict[str, Any]:
        result = self.core.run()
        if isinstance(result, dict) and "error" in result:
            return result

        result_df: pd.DataFrame = result  # type: ignore[assignment]

        self.repo.save_forecasts(result_df)

        anomaly_count = int(result_df["is_anomaly"].sum())
        repos_with_anomalies = int(result_df[result_df["is_anomaly"]].repo_id.nunique())

        print("\nDAILY DOW+MAD ACTIVITY ANALYSIS (v3, parallel)")
        print("===============================================")
        print(f"Lookback weeks: {self.core.lookback_weeks}")
        print(f"MAD threshold: {self.core.mad_threshold}")
        print(f"Total points: {len(result_df)}")
        print(f"Total anomalies: {anomaly_count}")
        print(f"Repos with anomalies: {repos_with_anomalies}")

        # Keep 'anomalies' key to match main.py expectations
        return {
            "total_points": int(len(result_df)),
            "anomalies": anomaly_count,
            "repos_with_anomalies": repos_with_anomalies,
        }