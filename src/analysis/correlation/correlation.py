# src/analysis/correlation/correlation.py
#!/usr/bin/env python3

from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from src.analysis.correlation.repo.repo import CorrelationRepository
from src.storage.unit_of_work import UnitOfWork


class CommitCorrelationCore:
    def __init__(self, repo: CorrelationRepository, n_workers: Optional[int] = None):
        self.repo = repo
        self.n_workers = n_workers

    # ---------- helpers ----------

    @staticmethod
    def _corr_safe(a: pd.Series, b: pd.Series) -> float | None:
        """
        Safe Pearson correlation: returns None if not enough data
        or if correlation is undefined (NaN).
        """
        if len(a) < 2 or len(b) < 2:
            return None
        try:
            val = a.corr(b)
            if pd.isna(val):
                return None
            return float(val)
        except Exception:
            return None

    # ---------- 1) WEEK vs COMMIT FREQUENCY ----------

    def _commit_frequency_correlation(self, commits: pd.DataFrame) -> Dict[str, Any]:
        if commits.empty:
            return {
                "commit_corr_week_of_year": None,
                "commit_corr_week_index": None,
                "n_weeks_commits": 0,
                "total_commits": 0,
            }

        commits = commits.copy()
        commits["commit_date"] = pd.to_datetime(commits["commit_date"])
        iso = commits["commit_date"].dt.isocalendar()
        commits["year"] = iso.year
        commits["week_of_year"] = iso.week

        weekly = (
            commits.groupby(["year", "week_of_year"], as_index=False)
            .size()
            .rename(columns={"size": "commit_count"})
            .sort_values(["year", "week_of_year"])
        )

        weekly["week_index"] = np.arange(1, len(weekly) + 1)

        corr_week_of_year = self._corr_safe(
            weekly["week_of_year"], weekly["commit_count"]
        )
        corr_week_index = self._corr_safe(
            weekly["week_index"], weekly["commit_count"]
        )

        return {
            "commit_corr_week_of_year": corr_week_of_year,
            "commit_corr_week_index": corr_week_index,
            "n_weeks_commits": int(len(weekly)),
            "total_commits": int(len(commits)),
        }

    # ---------- 2) WEEK vs PR -> COMMIT LEAD TIME ----------

    def _pr_to_commit_correlation(
            self, commits: pd.DataFrame, prs: pd.DataFrame
    ) -> Dict[str, Any]:
        # defaults
        empty_result = {
            "pr_corr_week_of_year": None,
            "pr_corr_week_index": None,
            "n_weeks_pr_lead": 0,
            "n_pr_merges_used": 0,
        }

        if commits.empty or prs.empty:
            return empty_result

        commits = commits.copy()
        prs = prs.copy()

        commits["pr_number"] = (
            commits["message"]
            .str.extract(r"Merge pull request #(\d+)", expand=False)
            .astype("Int64")
        )
        merges = commits.dropna(subset=["pr_number"])

        if merges.empty:
            return empty_result

        merged = merges.merge(
            prs,
            how="inner",
            on=["repo_id", "pr_number"],
            suffixes=("_commit", "_pr"),
        )

        if merged.empty:
            return empty_result

        merged["commit_date"] = pd.to_datetime(merged["commit_date"])
        merged["pr_created_at"] = pd.to_datetime(merged["pr_created_at"])

        merged["lead_time_hours"] = (
                                            merged["commit_date"] - merged["pr_created_at"]
                                    ).dt.total_seconds() / 3600.0

        merged = merged.dropna(subset=["lead_time_hours"])

        if merged.empty:
            return empty_result

        iso = merged["commit_date"].dt.isocalendar()
        merged["year"] = iso.year
        merged["week_of_year"] = iso.week

        weekly = (
            merged.groupby(["year", "week_of_year"], as_index=False)
            .agg(avg_lead_time_hours=("lead_time_hours", "mean"))
            .sort_values(["year", "week_of_year"])
        )

        weekly["week_index"] = np.arange(1, len(weekly) + 1)

        corr_week_of_year = self._corr_safe(
            weekly["week_of_year"], weekly["avg_lead_time_hours"]
        )
        corr_week_index = self._corr_safe(
            weekly["week_index"], weekly["avg_lead_time_hours"]
        )

        return {
            "pr_corr_week_of_year": corr_week_of_year,
            "pr_corr_week_index": corr_week_index,
            "n_weeks_pr_lead": int(len(weekly)),
            "n_pr_merges_used": int(len(merged)),
        }

    # ---------- PUBLIC API (per-repo, parallel) ----------

    def run_analysis(self) -> Dict[str, Any]:
        """
        Run correlation analysis for EACH repo and persist results.

        Returns:
            {
              "results": [ { "repo_id": ..., "commit_corr_week_of_year": ..., ... }, ... ],
              "repos_analyzed": <int>
            }
        """
        commits = self.repo.load_commits()
        prs = self.repo.load_pull_requests()

        if commits.empty and prs.empty:
            print("[CommitCorrelationCore] No commit or PR data found.")
            return {"error": "No commit or PR data found."}

        # Collect all repo_ids that appear in either commits or PRs
        repo_ids_commits = (
            set(commits["repo_id"].dropna().unique()) if not commits.empty else set()
        )
        repo_ids_prs = (
            set(prs["repo_id"].dropna().unique()) if not prs.empty else set()
        )
        repo_ids = sorted(repo_ids_commits | repo_ids_prs)

        all_results: List[Dict[str, Any]] = []

        def analyze_one_repo(repo_id: int) -> Dict[str, Any]:
            repo_commits = commits[commits["repo_id"] == repo_id].copy()
            repo_prs = prs[prs["repo_id"] == repo_id].copy()

            print(f"\n=== RUNNING CORRELATION FOR REPO {repo_id} ===")

            commit_corr = self._commit_frequency_correlation(repo_commits)
            pr_corr = self._pr_to_commit_correlation(repo_commits, repo_prs)

            result = {
                "repo_id": int(repo_id),
                **commit_corr,
                **pr_corr,
            }

            print("\n=== CORRELATION SUMMARY FOR REPO", repo_id, "===")
            print(result)

            # Save one result row per repo into DB
            self.repo.save_correlation_result(result)
            return result

        # Parallel mode if workers > 1, else serial loop
        if self.n_workers and self.n_workers > 1:
            print(f"[CommitCorrelationCore] Running in parallel with {self.n_workers} workers.")
            with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                for result in executor.map(analyze_one_repo, repo_ids):
                    all_results.append(result)
        else:
            print("[CommitCorrelationCore] Running in single-threaded mode.")
            for repo_id in repo_ids:
                result = analyze_one_repo(repo_id)
                all_results.append(result)

        print(
            f"[CommitCorrelationCore] Finished correlation for {len(all_results)} repos."
        )

        return {
            "results": all_results,
            "repos_analyzed": len(all_results),
        }


class CommitCorrelationAnalyzer:
    """
    High-level class used by main.py:

    commit_corr_analyzer = CommitCorrelationAnalyzer(database_url, workers)
    corr_results = commit_corr_analyzer.analyze()
    """

    def __init__(self, database_url: str, n_workers: int | None = None):
        self.database_url = database_url
        self.n_workers = n_workers

        # create correlation tables (commit_correlation_result)
        uow = UnitOfWork(database_url)
        uow.create_correlation_tables()

        repo = CorrelationRepository(database_url)
        self.core = CommitCorrelationCore(repo, n_workers=n_workers)

    def analyze(self) -> Dict[str, Any]:
        return self.core.run_analysis()
