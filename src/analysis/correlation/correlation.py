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

    @staticmethod
    def _kruskal_wallis_week_vs_commits(
        commits: pd.DataFrame,
        alpha: float = 0.05,
    ) -> Dict[str, Any]:

        empty = {
            "kw_h_stat": None,
            "kw_p_value": None,
            "eta_squared": None,
            "has_week_dependency": False,
            "n_weeks": 0,
            "n_week_groups": 0,
            "total_commits": int(len(commits)) if commits is not None else 0,
        }

        if commits is None or commits.empty:
            return empty

        df = commits.copy()
        df["commit_date"] = pd.to_datetime(df["commit_date"])

        iso = df["commit_date"].dt.isocalendar()
        df["year"] = iso.year
        df["week_of_year"] = iso.week

        weekly = (
            df.groupby(["year", "week_of_year"], as_index=False)
            .size()
            .rename(columns={"size": "commit_count"})
            .sort_values(["year", "week_of_year"])
        )

        if weekly.empty:
            return empty

        groups_df = weekly.groupby("week_of_year")["commit_count"]
        k = groups_df.ngroups
        N = len(weekly)

        if k < 2 or N <= k:
            result = empty.copy()
            result["n_weeks"] = int(N)
            result["n_week_groups"] = int(k)
            return result

        groups = [g.values.astype(float) for _, g in groups_df]
        group_sizes = [len(g) for g in groups]

        data = np.concatenate(groups)
        N_check = data.size
        if N_check != N:
            N = N_check

        ranks = pd.Series(data).rank(method="average").to_numpy()

        rank_sums = []
        start = 0
        for size in group_sizes:
            end = start + size
            rank_sums.append(ranks[start:end].sum())
            start = end

        numerator = 0.0
        for R_i, n_i in zip(rank_sums, group_sizes):
            numerator += (R_i ** 2) / n_i

        H = (12.0 / (N * (N + 1))) * numerator - 3.0 * (N + 1)

        _, tie_counts = np.unique(data, return_counts=True)
        denom = (N ** 3 - N)
        if denom != 0:
            tie_correction = 1.0 - ((tie_counts ** 3 - tie_counts).sum() / denom)
        else:
            tie_correction = 1.0

        if tie_correction > 0:
            H_corrected = H / tie_correction
        else:
            H_corrected = H

        df_chi = k - 1
        p_value = None
        try:
            from scipy.stats import chi2
            p_value = float(1.0 - chi2.cdf(H_corrected, df_chi))
        except Exception:
            p_value = None

        eta_sq = None
        if N > k:
            eta_raw = (H_corrected - (k - 1)) / (N - k)
            eta_sq = float(max(0.0, min(1.0, eta_raw)))

        has_dep = bool(p_value is not None and p_value < alpha)

        return {
            "kw_h_stat": float(H_corrected),
            "kw_p_value": p_value,
            "eta_squared": eta_sq,
            "has_week_dependency": has_dep,
            "n_weeks": int(N),
            "n_week_groups": int(k),
            "total_commits": int(len(df)),
        }


    def run_analysis(self) -> Dict[str, Any]:
        commits = self.repo.load_commits()

        if commits.empty:
            print("[CommitCorrelationCore] No commit data found.")
            return {"error": "No commit data found."}

        repo_ids = sorted(set(commits["repo_id"].dropna().unique()))

        all_results: List[Dict[str, Any]] = []

        def analyze_one_repo(repo_id: int) -> Dict[str, Any]:
            repo_commits = commits[commits["repo_id"] == repo_id].copy()

            print(f"\n=== RUNNING KRUSKAL-WALLIS FOR REPO {repo_id} ===")

            stats = self._kruskal_wallis_week_vs_commits(repo_commits)

            result = {
                "repo_id": int(repo_id),
                **stats,
            }

            print("\n=== KRUSKAL-WALLIS SUMMARY FOR REPO", repo_id, "===")
            print(result)

            self.repo.save_correlation_result(result)
            return result

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
            f"[CommitCorrelationCore] Finished Kruskal–Wallis analysis for "
            f"{len(all_results)} repos."
        )

        return {
            "results": all_results,
            "repos_analyzed": len(all_results),
        }


class CommitCorrelationAnalyzer:
    def __init__(self, database_url: str, n_workers: int | None = None):
        self.database_url = database_url
        self.n_workers = n_workers

        uow = UnitOfWork(database_url)
        uow.create_correlation_tables()

        repo = CorrelationRepository(database_url)
        self.core = CommitCorrelationCore(repo, n_workers=n_workers)

    def analyze(self) -> Dict[str, Any]:
        return self.core.run_analysis()
