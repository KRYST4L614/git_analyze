#!/usr/bin/env python3
import argparse
import os
import sys


def setup_imports():
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main():
    setup_imports()

    from src.analysis import RepositoryAnalyzer, LocationAnalyzer

    parser = argparse.ArgumentParser(
        description='Analyze GitHub repositories and contributors'
    )

    parser.add_argument(
        '--database-url',
        default='postgresql://postgres:password@localhost/github_analysis',
        help='PostgreSQL database URL'
    )

    parser.add_argument(
        '--workers',
        type=int,
        help='Number of parallel workers'
    )

    parser.add_argument(
        '--analyze',
        action='store_true',
        default=True,
        help='Analyze'
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

        print("\n" + "=" * 40)
        print("All analyses completed successfully!")
        print("Results saved to database")

    except Exception as e:
        print(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()