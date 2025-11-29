#!/usr/bin/env python3
import argparse
import os
import sys
from src.analysis import RepositoryAnalyzer, LocationAnalyzer
from src.analysis.correlation.correlation import CommitCorrelationAnalyzer
from src.analysis.activity.forecasting import RepositoryActivityAnalyzer



def setup_imports():
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main():
    setup_imports()

    parser = argparse.ArgumentParser(
        description='Analyze GitHub repositories and contributors'
    )

    parser.add_argument(
        '--database-url',
        default='postgresql://postgres:password@localhost/github_analysis',
        help='PostgreSQL database URL'
    )

    parser.add_argument(
        '--clusters',
        type=int,
        default=3,
        help='Number of repository clusters (default: 3)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        help='Number of parallel workers'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Chunk size for processing (default: 1000)'
    )

    parser.add_argument(
        '--analyze',
        action='store_true',
        default=True,
        help='Analyze'
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=2005,
        help='Chunk size for processing (default: 1000)'
    )
    args = parser.parse_args()

    try:
        print("Starting GitHub Data Analysis")
        print("=" * 40)

        # Анализ репозиториев
        if args.analyze:
            print("\nREPOSITORY TYPE ANALYSIS")
            print("-" * 25)
            repo_analyzer = RepositoryAnalyzer(args.database_url, args.workers)
            repo_results = repo_analyzer.analyze()

            if 'error' in repo_results:
                print(f"Repository analysis failed: {repo_results['error']}")
            else:
               print("Repository analysis completed successfully.")

            print("\nCONTRIBUTOR LOCATION ANALYSIS")
            print("-" * 30)
            location_analyzer = LocationAnalyzer(args.database_url, args.workers)
            location_results = location_analyzer.analyze()

            if 'error' in location_results:
                print(f"Location analysis failed: {location_results['error']}")
            else:
                print("Location analysis completed successfully.")

            print("\nCOMMIT ACTIVITY ANALYSIS (WEEK VS FREQUENCY)")
            print("-" * 40)
            commit_corr_analyzer = CommitCorrelationAnalyzer(args.database_url, args.workers)
            corr_results = commit_corr_analyzer.analyze()

            if 'error' in corr_results:
                print(f"Commit correlation analysis failed: {corr_results['error']}")
            else:
                print("Commit correlation analysis completed successfully.")
                print(f"Repositories analyzed: {corr_results['repos_analyzed']}")

            print("\nREPOSITORY ACTIVITY FORECAST & ANOMALY DETECTION")
            print("-" * 50)

            activity_analyzer = RepositoryActivityAnalyzer(args.database_url, args.workers, args.start_year)
            activity_results = activity_analyzer.analyze()

            if 'error' in activity_results:
                print(f"Activity analysis failed: {activity_results['error']}")
            else:
                print("Activity analysis completed successfully.")
                print(f"Repo-years total: {activity_results['repo_years_total']}")
                print(f"Alive repo-years: {activity_results['repo_years_alive']}")
                print(f"Anomalous weeks: {activity_results['anomalous_weeks']}")

        print("\n" + "=" * 40)
        print("All analyses completed successfully!")
        print("Results saved to database")

    except Exception as e:
        print(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()