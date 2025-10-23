#!/usr/bin/env python3
"""
GitHub Dataset Tool
A unified tool for collecting and analyzing GitHub repository data.
"""

import argparse
import os
import sys
import time
from datetime import datetime

from data.github.github_collector import GitHubDatasetCollector
from utils.utils import save_to_csv


def setup_imports():
    """Setup proper imports by adding src to path"""
    src_dir = os.path.dirname(__file__)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def get_collect_parser():
    """Parser for data collection command"""
    parser = argparse.ArgumentParser(
        description='Collect GitHub repository data',
        add_help=False
    )

    parser.add_argument(
        '--token',
        help='GitHub API token (or use GITHUB_TOKEN env variable)',
        default=os.getenv('GITHUB_TOKEN') or os.getenv('GH_TOKEN')
    )

    parser.add_argument(
        '--repos',
        type=int,
        default=100,
        help='Maximum repositories to analyze (default: 100)'
    )

    parser.add_argument(
        '--contributors',
        type=int,
        default=50,
        help='Maximum contributors per repository (default: 50)'
    )

    parser.add_argument(
        '--min-contributions',
        type=int,
        default=100,
        help='Minimum contributions per contributor (default: 100)'
    )

    parser.add_argument(
        '--min-commits',
        type=int,
        default=1000,
        help='Minimum commits per repository (default: 1000)'
    )

    parser.add_argument(
        '--max-commits',
        type=int,
        default=1000,
        help='Maximum commits per user (default: 1000)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=50,
        help='Number of worker threads (default: 50)'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output CSV filename'
    )

    return parser


def get_analyze_parser():
    """Parser for data analysis command"""
    parser = argparse.ArgumentParser(
        description='Analyze collected GitHub data',
        add_help=False
    )

    parser.add_argument(
        '--input', '-i',
        required=True,
        help='Input CSV file with collected data'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output analysis file'
    )

    parser.add_argument(
        '--analysis-type',
        choices=['stats', 'trends', 'correlations'],
        default='stats',
        help='Type of analysis to perform (default: stats)'
    )

    return parser


def main():
    """Main entry point with subcommands"""
    setup_imports()

    parser = argparse.ArgumentParser(
        description='GitHub Dataset Tool - Collect and analyze GitHub repository data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect data
  python main.py collect --token ghp_yourtoken123 --repos 50

  # Collect with custom settings
  python main.py collect --token ghp_yourtoken123 --repos 100 --min-commits 2000 --output my_data.csv

  # Using environment variable for token
  export GITHUB_TOKEN=ghp_yourtoken123
  python main.py collect --repos 30
        """
    )

    subparsers = parser.add_subparsers(
        dest='command',
        title='commands',
        description='available commands',
        help='additional help'
    )

    # Collect command
    collect_parser = subparsers.add_parser(
        'collect',
        help='Collect GitHub repository data',
        parents=[get_collect_parser()]
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    try:
        if args.command == 'collect':
            run_collect(args)

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        if hasattr(args, 'verbose') and args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def run_collect(args):
    """Run data collection"""
    if not args.token:
        print("Error: GitHub token is required for data collection.")
        print("Use --token argument or set GITHUB_TOKEN environment variable.")
        sys.exit(1)

    print("Starting GitHub data collection...")
    print("Configuration:")
    print(f"  Repositories: {args.repos}")
    print(f"  Contributors per repo: {args.contributors}")
    print(f"  Min contributions: {args.min_contributions}")
    print(f"  Min commits per repo: {args.min_commits}")
    print(f"  Worker threads: {args.workers}")
    print()

    collector = GitHubDatasetCollector(
        token=args.token,
        max_workers=args.workers,
        max_contributors=args.contributors,
        min_contributions=args.min_contributions,
        max_repos=args.repos,
        min_commits_per_repo=args.min_commits,
        max_commits_per_user=args.max_commits,
    )

    start_time = time.time()
    data = collector.collect_repos()
    end_time = time.time()

    print(f"Collection completed in {end_time - start_time:.2f} seconds")

    if args.output:
        output_filename = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"github_data_{timestamp}.csv"

    save_to_csv(data, output_filename)

    if data:
        unique_repos = len(set(item['repo_id'] for item in data))
        unique_contributors = len(set(item['contributor_login'] for item in data))

        print(f"\nCollection Summary:")
        print(f"  Unique repositories: {unique_repos}")
        print(f"  Unique contributors: {unique_contributors}")
        print(f"  Total records: {len(data)}")
        print(f"  Output file: {output_filename}")


if __name__ == "__main__":
    main()
