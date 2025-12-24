import argparse
import os
import sys


def setup_imports():
    src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def main():
    setup_imports()

    from src.analysis import LocationAnalyzer, RepositoryAnalyzer

    parser = argparse.ArgumentParser(
        description='Analyze GitHub repositories and contributors'
    )

    parser.add_argument(
        '--database-url',
        default='jdbc:postgresql://postgres:5432/github_analysis',
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
        help='Run all analyses'
    )

    parser.add_argument(
        '--type',
        action='store_true',
        default=True,
        help='Make type clustering for repositories'
    )

    parser.add_argument(
        '--location',
        action='store_true',
        default=True,
        help='Make location clustering for repositories'
    )

    parser.add_argument(
        '--spark-master',
        default='spark://spark-master:7077',
        help='Spark master URL'
    )

    parser.add_argument(
        '--spark-partitions',
        type=int,
        default=10,
        help='Number of Spark partitions'
    )

    args = parser.parse_args()

    try:
        print("Starting GitHub Data Analysis")
        print("=" * 40)

        if args.analyze:
            if args.type:
                print("\nSPARK REPOSITORY TYPE ANALYSIS")
                print("-" * 30)
                spark_analyzer = RepositoryAnalyzer(
                    database_url=args.database_url,
                    spark_master=args.spark_master,
                    n_partitions=args.spark_partitions
                )
                repo_results = spark_analyzer.analyze()

                if 'error' in repo_results:
                    print(f"Spark repository analysis failed: {repo_results['error']}")
                else:
                    print("Spark repository analysis completed successfully.")

                spark_analyzer.stop()
            if args.location:
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