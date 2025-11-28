#!/usr/bin/env python3
import argparse
import os
import sys
import yaml

from pathlib import Path


def load_config(args):
    """Load configuration from source"""
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
    """Merge command line arguments with config file values"""
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
    """Get the default value for an argument"""
    defaults = {
        'token': os.getenv('GITHUB_TOKEN'),
        'repos': 50,
        'workers': 10,
        'database_url': 'postgresql://postgres:password@localhost/github_analysis'
    }
    return defaults.get(arg_name)


def setup_imports():
    """Setup proper imports"""
    src_dir = os.path.dirname(__file__)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main():
    """Main entry point"""
    setup_imports()

    parser = argparse.ArgumentParser(
        description='GitHub Dataset Tool - Collect GitHub repository data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic collection
  python main.py --token ghp_yourtoken123 --repos 10 --workers 4

  # Using config file
  python main.py --config config.yml
      """
    )

    parser.add_argument(
        '--config',
        help='Path to configuration file (YAML/YML)',
        type=str
    )

    parser.add_argument(
        '--token',
        help='GitHub API token (or use GITHUB_TOKEN env variable)',
        default=os.getenv('GITHUB_TOKEN')
    )

    parser.add_argument(
        '--repos',
        type=int,
        default=50,
        help='Maximum repositories to collect (default: 50)'
    )

    parser.add_argument(
        '--workers', '-p',
        type=int,
        default=10,
        help='Number of workers'
    )

    parser.add_argument(
        '--database-url',
        help='PostgreSQL database URL in format:  postgresql://(psql-user):(psql-passwd)@(host)/(database)',
        default='postgresql://postgres:password@localhost/github_analysis'
    )

    args = parser.parse_args()

    config = load_config(args)

    fin_args = merge_config_with_args(args, config)

    try:
        run_collection(fin_args)

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def run_collection(args):
    """Run collection"""
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


if __name__ == "__main__":
    main()
