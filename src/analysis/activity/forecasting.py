from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from src.analysis.activity.repo.repo import ActivityRepository
from src.storage.unit_of_work import UnitOfWork


class RepositoryActivityCore:
    def __init__(self, repo: ActivityRepository, n_workers: Optional[int] = None, start_year: int = 2005):
        self.repo = repo
        self.n_workers = n_workers
        self.start_year = start_year

    # ----------------------- LIVENESS CRITERIA -----------------------

    @staticmethod
    def _compute_year_stats_for_repo(
        repo_id: int,
        year: int,
        df_year: pd.DataFrame,
    ) -> Dict[str, Any]:
        total_commits = int(len(df_year))
        if total_commits == 0:
            return {
                "repo_id": int(repo_id),
                "year": int(year),
                "total_commits": 0,
                "active_weeks": 0,
                "first_week": None,
                "last_week": None,
                "weeks_in_range": 0,
                "active_weeks_ratio": 0.0,
                "is_alive": False,
            }

        weekly_counts = (
            df_year.groupby("iso_week", as_index=False)
            .size()
            .rename(columns={"size": "commit_count"})
        )

        active_weeks = int(len(weekly_counts))
        first_week = int(weekly_counts["iso_week"].min())
        last_week = int(weekly_counts["iso_week"].max())
        weeks_in_range = last_week - first_week + 1 if last_week >= first_week else active_weeks

        active_ratio = float(active_weeks / weeks_in_range) if weeks_in_range > 0 else 0.0

        # Liveness criteria:
        # - at least 10 commits in this year;
        # - activity in at least 4 weeks;
        # - at least 20% of weeks between first and last active week are non-zero.
        is_alive = (
            total_commits >= 10
            and active_weeks >= 4
            and active_ratio >= 0.20
        )

        return {
            "repo_id": int(repo_id),
            "year": int(year),
            "total_commits": total_commits,
            "active_weeks": active_weeks,
            "first_week": first_week,
            "last_week": last_week,
            "weeks_in_range": int(weeks_in_range),
            "active_weeks_ratio": active_ratio,
            "is_alive": bool(is_alive),
        }

    @staticmethod
    def _build_full_weekly_series(
        df_year: pd.DataFrame,
        first_week: int,
        last_week: int,
    ) -> pd.Series:
        weekly_counts = (
            df_year.groupby("iso_week", as_index=False)
            .size()
            .rename(columns={"size": "commit_count"})
        )
        weekly_series = pd.Series(
            0,
            index=pd.Index(range(first_week, last_week + 1), name="iso_week"),
            dtype=int,
        )
        for _, row in weekly_counts.iterrows():
            weekly_series.at[int(row["iso_week"])] = int(row["commit_count"])
        return weekly_series

    @staticmethod
    def _robust_z_scores(values: np.ndarray) -> np.ndarray:
        if values.size == 0:
            return np.array([], dtype=float)

        log_vals = np.log1p(values.astype(float))
        median = np.median(log_vals)
        abs_dev = np.abs(log_vals - median)
        mad = np.median(abs_dev)

        if mad < 1e-9:
            # Fallback to standard deviation if MAD is (almost) zero.
            mean = float(np.mean(log_vals))
            std = float(np.std(log_vals))
            if std < 1e-9:
                # Completely flat series: no anomalies.
                return np.zeros_like(log_vals, dtype=float)
            return (log_vals - mean) / std

        robust_std = 1.4826 * mad
        return (log_vals - median) / robust_std

    def _detect_anomalies_for_repo_year(
        self,
        repo_id: int,
        year: int,
        df_year: pd.DataFrame,
        year_stats: Dict[str, Any],
        z_threshold: float = 2.5,
    ) -> List[Dict[str, Any]]:
        if not year_stats.get("is_alive", False):
            return []

        first_week = year_stats["first_week"]
        last_week = year_stats["last_week"]
        if first_week is None or last_week is None or last_week < first_week:
            return []

        weekly_series = self._build_full_weekly_series(df_year, first_week, last_week)
        z_scores = self._robust_z_scores(weekly_series.values)

        anomalies: List[Dict[str, Any]] = []
        for week_idx, (week, count, z) in enumerate(zip(weekly_series.index, weekly_series.values, z_scores)):
            if abs(z) >= z_threshold:
                direction = "high" if z > 0 else "low"
                anomalies.append(
                    {
                        "repo_id": int(repo_id),
                        "year": int(year),
                        "iso_week": int(week),
                        "commit_count": int(count),
                        "z_score": float(z),
                        "direction": direction,
                        "total_commits_year": int(year_stats["total_commits"]),
                        "active_weeks_year": int(year_stats["active_weeks"]),
                    }
                )

        return anomalies

    def run_analysis(self) -> Dict[str, Any]:
        commits = self.repo.load_corporate_commits()

        if commits.empty:
            print("[RepositoryActivityCore] No commit data found for corporate repositories.")
            return {"error": "No commit data found for corporate repositories."}

        commits["commit_date"] = pd.to_datetime(commits["commit_date"])
        iso = commits["commit_date"].dt.isocalendar()
        commits["iso_year"] = iso.year.astype(int)
        commits["iso_week"] = iso.week.astype(int)

        commits = commits[commits["iso_year"] >= self.start_year].copy()
        if commits.empty:
            print(f"[RepositoryActivityCore] No commits for years >= {self.start_year}.")
            return {"error": f"No commits for years >= {self.start_year}."}

        repo_ids = sorted(commits["repo_id"].dropna().unique())
        print(
            f"[RepositoryActivityCore] Running activity analysis for "
            f"{len(repo_ids)} corporate repositories starting from {self.start_year}."
        )

        all_year_stats: List[Dict[str, Any]] = []
        all_anomalies: List[Dict[str, Any]] = []

        def analyze_one_repo(repo_id: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
            repo_df = commits[commits["repo_id"] == repo_id]
            repo_years = sorted(repo_df["iso_year"].unique())

            repo_year_stats: List[Dict[str, Any]] = []
            repo_anomalies: List[Dict[str, Any]] = []

            for year in repo_years:
                df_year = repo_df[repo_df["iso_year"] == year]
                year_stats = self._compute_year_stats_for_repo(repo_id, year, df_year)
                repo_year_stats.append(year_stats)

                if year_stats["is_alive"]:
                    anomalies = self._detect_anomalies_for_repo_year(
                        repo_id=repo_id,
                        year=year,
                        df_year=df_year,
                        year_stats=year_stats,
                    )
                    repo_anomalies.extend(anomalies)

            # Persist to DB
            if repo_year_stats:
                self.repo.save_year_stats(repo_year_stats)
            if repo_anomalies:
                self.repo.save_weekly_anomalies(repo_anomalies)

            print(
                f"[RepositoryActivityCore] Repo {repo_id}: "
                f"{sum(1 for s in repo_year_stats if s['is_alive'])} alive years, "
                f"{len(repo_anomalies)} anomalies."
            )

            return repo_year_stats, repo_anomalies

        if self.n_workers and self.n_workers > 1:
            print(f"[RepositoryActivityCore] Running in parallel with {self.n_workers} workers.")
            with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                for repo_year_stats, repo_anomalies in executor.map(analyze_one_repo, repo_ids):
                    all_year_stats.extend(repo_year_stats)
                    all_anomalies.extend(repo_anomalies)
        else:
            print("[RepositoryActivityCore] Running in single-threaded mode.")
            for repo_id in repo_ids:
                repo_year_stats, repo_anomalies = analyze_one_repo(repo_id)
                all_year_stats.extend(repo_year_stats)
                all_anomalies.extend(repo_anomalies)

        alive_years = sum(1 for s in all_year_stats if s["is_alive"])
        print(
            "[RepositoryActivityCore] Finished activity analysis.\n"
            f"  Total repo-years: {len(all_year_stats)}\n"
            f"  Alive repo-years: {alive_years}\n"
            f"  Anomalous weeks:  {len(all_anomalies)}"
        )

        return {
            "repo_years_total": len(all_year_stats),
            "repo_years_alive": alive_years,
            "anomalous_weeks": len(all_anomalies),
        }


class RepositoryActivityAnalyzer:
    def __init__(self, database_url: str, n_workers: Optional[int] = None, start_year: int = 2005):
        self.database_url = database_url
        self.n_workers = n_workers
        self.start_year = start_year

        # Ensure activity tables exist
        uow = UnitOfWork(database_url)
        uow.create_activity_tables()

        repo = ActivityRepository(database_url)
        self.core = RepositoryActivityCore(repo, n_workers=n_workers, start_year=start_year)

    def analyze(self) -> Dict[str, Any]:
        return self.core.run_analysis()
