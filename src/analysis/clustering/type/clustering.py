import logging
from typing import Dict, Any

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *

from dependencies.src.udf_functions import (
    extract_organization_udf,
    calculate_corporate_weight_udf,
    calculate_educational_weight_udf,
    calculate_personal_weight_udf,
    determine_repository_type_udf
)
from src.analysis.clustering.type.repo.repo import AnalysisRepository
from src.analysis.spark.config import SparkConfig

logger = logging.getLogger(__name__)


def _print_results_summary(analysis: Dict):
    print("\n" + "=" * 50)
    print("SPARK CLASSIFICATION RESULTS SUMMARY")
    print("=" * 50)

    print("\nType Distribution:")
    for repo_type, count in analysis['type_distribution'].items():
        percentage = (count / analysis['total_repositories']) * 100
        print(f"  {repo_type.upper():<12}: {count:>6} repositories ({percentage:>5.1f}%)")

    print(f"\nConfidence Statistics:")
    print(f"  High confidence (>0.7):   {analysis['confidence_stats']['high_confidence']:>6}")
    print(f"  Medium confidence (0.4-0.7): {analysis['confidence_stats']['medium_confidence']:>6}")
    print(f"  Low confidence (<=0.4):   {analysis['confidence_stats']['low_confidence']:>6}")
    print(f"  Average confidence: {analysis['confidence_stats']['avg_confidence']:.3f}")

    print(f"\nTotal processed: {analysis['total_repositories']} repositories")

    print("\nDetailed Type Statistics:")
    for repo_type, stats in analysis['type_stats'].items():
        print(f"\n  {repo_type.upper()}:")
        print(f"    Count: {stats['count']}")
        print(f"    Avg Corporate Weight: {stats['avg_corporate_weight']:.2f}")
        print(f"    Avg Educational Weight: {stats['avg_educational_weight']:.2f}")
        print(f"    Avg Personal Weight: {stats['avg_personal_weight']:.2f}")
        print(f"    Avg Confidence: {stats['avg_confidence']:.3f}")


def show_sample_results(results_df: DataFrame, sample_size: int = 10):
    logger.info("Displaying sample results...")

    sample_df = (
        results_df.select(
            "repo_id",
            "full_name",
            "final_type",
            "confidence_score",
            "corporate_weight",
            "educational_weight",
            "personal_weight",
            "org_name"
        ).limit(sample_size))

    print("\nSample repository classification:")
    print("=" * 80)
    for row in sample_df.collect():
        print(f"Repository: {row['full_name']}")
        print(f"  Type: {row['final_type']} (confidence: {row['confidence_score']:.3f})")
        print(f"  Org: {row['org_name']}")
        print(f"  Weights: corp={row['corporate_weight']:.1f}, "
              f"edu={row['educational_weight']:.1f}, pers={row['personal_weight']:.1f}")
        print("-" * 80)


def analyze_results(results_df: DataFrame) -> Dict[str, Any]:
    logger.info("Analyzing classification results...")

    type_distribution_df = results_df.groupBy("final_type").count()
    type_stats_df = (results_df
    .groupBy("final_type")
    .agg(
        F.avg("corporate_weight").alias("avg_corporate_weight"),
        F.avg("educational_weight").alias("avg_educational_weight"),
        F.avg("personal_weight").alias("avg_personal_weight"),
        F.avg("confidence_score").alias("avg_confidence"),
        F.count("repo_id").alias("count")
    ))

    confidence_stats_df = results_df.select(
        F.sum(F.when(F.col("confidence_score") > 0.7, 1).otherwise(0)).alias("high_confidence"),
        F.sum(F.when((F.col("confidence_score") > 0.4) & (F.col("confidence_score") <= 0.7), 1).otherwise(0)).alias(
            "medium_confidence"),
        F.sum(F.when(F.col("confidence_score") <= 0.4, 1).otherwise(0)).alias("low_confidence"),
        F.avg("confidence_score").alias("avg_confidence"),
        F.count("repo_id").alias("total_repositories")
    )

    type_distribution = {row['final_type']: row['count'] for row in type_distribution_df.collect()}
    type_stats = {}
    for row in type_stats_df.collect():
        type_stats[row['final_type']] = {
            'avg_corporate_weight': row['avg_corporate_weight'],
            'avg_educational_weight': row['avg_educational_weight'],
            'avg_personal_weight': row['avg_personal_weight'],
            'avg_confidence': row['avg_confidence'],
            'count': row['count']
        }

    confidence_row = confidence_stats_df.collect()[0]
    confidence_stats = {
        'high_confidence': confidence_row['high_confidence'],
        'medium_confidence': confidence_row['medium_confidence'],
        'low_confidence': confidence_row['low_confidence'],
        'avg_confidence': confidence_row['avg_confidence']
    }

    analysis_results = {
        'type_distribution': type_distribution,
        'type_stats': type_stats,
        'confidence_stats': confidence_stats,
        'total_repositories': confidence_row['total_repositories']
    }

    return analysis_results


class RepositoryClassifier:
    def __init__(self, database_url: str, spark_master: str = None, n_partitions: int = 10):
        self.database_url = database_url
        self.n_partitions = n_partitions

        self.spark = SparkConfig.get_spark_session(
            app_name="GitHubRepositoryClassifier",
            spark_master=spark_master,
            n_partitions=n_partitions
        )

        self.repository = AnalysisRepository(
            spark_session=self.spark,
            database_url=database_url
        )

        self._register_udfs()

    def _register_udfs(self):
        self.extract_org_udf = F.udf(extract_organization_udf, StringType())
        self.corp_weight_udf = F.udf(calculate_corporate_weight_udf, FloatType())
        self.edu_weight_udf = F.udf(calculate_educational_weight_udf, FloatType())
        self.pers_weight_udf = F.udf(calculate_personal_weight_udf, FloatType())
        self.determine_type_udf = F.udf(determine_repository_type_udf,
                                        StructType([
                                            StructField("repo_type", StringType()),
                                            StructField("confidence", FloatType())
                                        ]))

    def process_repositories(self) -> DataFrame:
        logger.info("Starting Spark-based repository classification...")

        repos_df = self.repository.load_repository_data(self.n_partitions)

        processed_df = (repos_df
                        .withColumn("org_name", self.extract_org_udf(F.col("full_name")))
                        .withColumn("corporate_weight",
                                    self.corp_weight_udf(F.col("description"), F.col("topics"), F.col("org_name")))
                        .withColumn("educational_weight",
                                    self.edu_weight_udf(F.col("full_name"), F.col("description"), F.col("topics")))
                        .withColumn("personal_weight",
                                    self.pers_weight_udf(F.col("full_name"), F.col("description"), F.col("topics"),
                                                         F.col("corporate_weight"), F.col("educational_weight")))
                        .withColumn("type_info",
                                    self.determine_type_udf(F.col("corporate_weight"),
                                                            F.col("educational_weight"),
                                                            F.col("personal_weight")))
                        .withColumn("final_type", F.col("type_info.repo_type"))
                        .withColumn("confidence_score", F.col("type_info.confidence"))
                        .drop("type_info"))

        return processed_df.cache()

    def run_analysis(self) -> Dict[str, Any]:
        try:
            logger.info("Starting Spark repository classification analysis...")

            results_df = self.process_repositories()

            show_sample_results(results_df)

            analysis = analyze_results(results_df)

            self.repository.save_clustering_results(results_df)

            _print_results_summary(analysis)

            return {
                'analysis': analysis,
                'processed_repositories': analysis['total_repositories'],
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"Error during Spark analysis: {e}")
            return {
                'error': str(e),
                'status': 'error'
            }
        finally:
            if 'results_df' in locals():
                results_df.unpersist()
                logger.info("Cleared cached DataFrames")

    def stop(self):
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")


class RepositoryAnalyzer:
    def __init__(self, database_url: str, spark_master: str = None, n_partitions: int = 10):
        self.database_url = database_url
        self.spark_master = spark_master
        self.n_partitions = n_partitions
        self.classifier = None

    def analyze(self) -> Dict:
        try:
            logger.info(f"Initializing Spark classifier with master: {self.spark_master}")
            self.classifier = RepositoryClassifier(
                database_url=self.database_url,
                spark_master=self.spark_master,
                n_partitions=self.n_partitions
            )

            return self.classifier.run_analysis()

        except Exception as e:
            logger.error(f"Error in SparkRepositoryAnalyzer: {e}")
            return {
                'error': str(e),
                'status': 'error'
            }

    def stop(self):
        if self.classifier:
            self.classifier.stop()
