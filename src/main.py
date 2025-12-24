#!/usr/bin/env python3
import argparse
import os
import sys
import yaml

from pathlib import Path


def load_config(args):
    if not args.config:
        return {}

    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {args.config}")

    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"Warning: Could not load config from {config_path}: {e}. Input supports only YAML/YML format!")


def merge_config_with_args(args, config):
    final_args = argparse.Namespace(**vars(args))

    arg_config_map = {
        'token': 'token',
        'repos': 'repos',
        'workers': 'workers',
        'database_url': 'database_url'
    }

    for arg_name, config_key in arg_config_map.items():
        config_value = config.get(config_key)
        if config_value is not None:
            current_value = getattr(args, arg_name)
            default_value = get_default_value(arg_name)
            if current_value == default_value:
                setattr(final_args, arg_name, config_value)

    return final_args


def get_default_value(arg_name):
    defaults = {
        'token': os.getenv('GITHUB_TOKEN'),
        'repos': 50,
        'workers': 10,
        'database_url': 'postgresql://postgres:password@localhost/github_analysis'
    }
    return defaults.get(arg_name)


def setup_imports():
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def run_collection(args):
    if not args.token:
        print("Error: GitHub token is required for data collection.")
        print("Use --token argument or set GITHUB_TOKEN environment variable.")
        print("You can get a token from: https://github.com/settings/tokens")
        sys.exit(1)

    print("Starting GitHub Data Collection")
    print("=" * 50)
    print(f"Repositories to collect: {args.repos}")
    print(f"Workers: {args.workers or 'CPU count'}")
    print()

    from src.data.github.github_collector import GitHubDatasetCollector

    collector = GitHubDatasetCollector(
        token=args.token,
        max_workers=args.workers,
        max_repos=args.repos,
        database_url=args.database_url
    )

    try:
        results = collector.collect_repos()

        print("=" * 50)
        print("Collection Completed!")
        print(f"Processed {len(results)} repositories successfully")

    except Exception as e:
        print("=" * 50)
        print("Collection Failed!")
        print(f"Error: {e}")
        sys.exit(1)


def run_analysis(args):
    try:
        print("Starting GitHub Data Analysis")
        print("=" * 40)

        from src.analysis import RepositoryAnalyzer, LocationAnalyzer

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


def main():
    setup_imports()

    parser = argparse.ArgumentParser(
        description='GitHub Dataset Tool - Collect and analyze GitHub repository data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect and analyze data (full pipeline)
  python main.py --token ghp_yourtoken123 --repos 10

  # Collect data only
  python main.py collect --token ghp_yourtoken123 --repos 10

  # Analyze data only
  python main.py analyze --database-url postgresql://user:pass@localhost/db

  # Using config file
  python main.py --config config.yml
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    collect_parser = subparsers.add_parser('collect', help='Collect GitHub repository data only')
    collect_parser.add_argument('--config', help='Path to configuration file (YAML/YML)', type=str)
    collect_parser.add_argument('--token', help='GitHub API token (or use GITHUB_TOKEN env variable)',
                                default=os.getenv('GITHUB_TOKEN'))
    collect_parser.add_argument('--repos', type=int, default=50, help='Maximum repositories to collect (default: 50)')
    collect_parser.add_argument('--workers', '-p', type=int, default=10, help='Number of workers')
    collect_parser.add_argument('--database-url',
                                help='PostgreSQL database URL in format: postgresql://(psql-user):(psql-passwd)@(host)/(database)',
                                default='postgresql://postgres:password@localhost/github_analysis')

    analyze_parser = subparsers.add_parser('analyze', help='Analyze collected data only')
    analyze_parser.add_argument('--database-url', default='postgresql://postgres:password@localhost/github_analysis',
                                help='PostgreSQL database URL')
    analyze_parser.add_argument('--workers', type=int, help='Number of parallel workers')
    analyze_parser.add_argument('--analyze', action='store_true', default=True, help='Analyze')

    parser.add_argument('--config', help='Path to configuration file (YAML/YML)', type=str)
    parser.add_argument('--token', help='GitHub API token (or use GITHUB_TOKEN env variable)',
                        default=os.getenv('GITHUB_TOKEN'))
    parser.add_argument('--repos', type=int, default=50, help='Maximum repositories to collect (default: 50)')
    parser.add_argument('--workers', '-p', type=int, default=10, help='Number of workers')
    parser.add_argument('--database-url',
                        help='PostgreSQL database URL in format: postgresql://(psql-user):(psql-passwd)@(host)/(database)',
                        default='postgresql://postgres:password@localhost/github_analysis')
    parser.add_argument('--analyze-workers', type=int, help='Number of parallel workers for analysis')

    args = parser.parse_args()

    try:
        if args.command == 'collect':
            config = load_config(args)
            fin_args = merge_config_with_args(args, config)
            run_collection(fin_args)
        elif args.command == 'analyze':
            run_analysis(args)
        else:
            print("=" * 60)
            print("Running full pipeline: collect followed by analyze")
            print("=" * 60)

            config = load_config(args)
            fin_args = merge_config_with_args(args, config)
            run_collection(fin_args)

            print("\n" + "=" * 60)
            print("Starting analysis phase")
            print("=" * 60)

            analyze_args = argparse.Namespace(
                database_url=fin_args.database_url,
                workers=args.analyze_workers if args.analyze_workers else fin_args.workers,
                analyze=True
            )
            run_analysis(analyze_args)

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()