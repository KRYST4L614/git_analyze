import logging
from typing import Dict, Any

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *

from src.analysis.spark.config import SparkConfig
from src.analysis.clustering.place.repo.repo import LocationRepository
from dependencies.src.geo_udf_functions import (
    geocode_location_udf,
    extract_country_udf,
    extract_city_udf
)

logger = logging.getLogger(__name__)


class LocationClassifier:
    def __init__(self, database_url: str, spark_master: str = None, n_partitions: int = 10):
        self.database_url = database_url
        self.n_partitions = n_partitions

        self.spark = SparkConfig.get_spark_session(
            app_name="GitHubLocationClassifier",
            spark_master=spark_master,
            n_partitions=n_partitions
        )

        self.repository = LocationRepository(
            spark_session=self.spark,
            database_url=database_url
        )

        self._register_udfs()

    def _register_udfs(self):
        self.geocode_udf = F.udf(geocode_location_udf,
                                 StructType([
                                     StructField("latitude", DoubleType()),
                                     StructField("longitude", DoubleType()),
                                     StructField("country", StringType()),
                                     StructField("city", StringType())
                                 ]))

        self.extract_country_udf = F.udf(extract_country_udf, StringType())
        self.extract_city_udf = F.udf(extract_city_udf, StringType())

    def process_contributor_locations(self) -> DataFrame:
        logger.info("Starting Spark-based contributor location analysis...")

        contributors_df = self.repository.load_contributor_location_data(self.n_partitions)

        logger.info(f"Loaded {contributors_df.count()} contributors with location data")

        processed_df = (contributors_df
                        .filter(F.col("location").isNotNull() & (F.col("location") != ""))
                        .withColumn("geodata", self.geocode_udf(F.col("location")))
                        .withColumn("country_name", self.extract_country_udf(F.col("geodata")))
                        .withColumn("city_name", self.extract_city_udf(F.col("geodata")))
                        .withColumn("latitude", F.col("geodata.latitude"))
                        .withColumn("longitude", F.col("geodata.longitude"))
                        .withColumn("confidence",
                                    F.when(F.col("country_name").isNotNull(), 1.0).otherwise(0.0))
                        .withColumn("original_location", F.col("location"))
                        .drop("geodata", "location")
                        )

        successful_df = processed_df.filter(F.col("country_name").isNotNull())

        logger.info(f"Successfully geocoded {successful_df.count()} locations")

        return successful_df.cache()

    def run_analysis(self) -> Dict[str, Any]:
        try:
            logger.info("Starting Spark contributor location analysis...")

            results_df = self.process_contributor_locations()

            analysis = self._analyze_results(results_df)

            self.repository.save_contributor_locations(results_df)

            self._print_results_summary(analysis)

            return {
                'analysis': analysis,
                'processed_contributors': analysis['total_contributors'],
                'successful_locations': analysis['successful_locations'],
                'status': 'success'
            }

        except Exception as e:
            logger.error(f"Error during Spark location analysis: {e}")
            return {
                'error': str(e),
                'status': 'error'
            }
        finally:
            if 'results_df' in locals():
                results_df.unpersist()
                logger.info("Cleared cached DataFrames")

    def _analyze_results(self, results_df: DataFrame) -> Dict[str, Any]:

        country_stats = (results_df
                         .groupBy("country_name")
                         .agg(
            F.count("contributor_id").alias("count"),
            F.avg("confidence").alias("avg_confidence")
        )
                         .orderBy(F.desc("count"))
                         )

        total_stats = results_df.agg(
            F.count("contributor_id").alias("total_contributors"),
            F.avg("confidence").alias("avg_confidence"),
            F.count_distinct("country_name").alias("unique_countries"),
            F.count_distinct("city_name").alias("unique_cities")
        ).collect()[0]

        top_countries = {row['country_name']: row['count']
                         for row in country_stats.limit(10).collect()}

        return {
            'total_contributors': total_stats['total_contributors'],
            'successful_locations': total_stats['total_contributors'],
            'unique_countries': total_stats['unique_countries'],
            'unique_cities': total_stats['unique_cities'],
            'avg_confidence': float(total_stats['avg_confidence']),
            'top_countries': top_countries
        }

    def _print_results_summary(self, analysis: Dict[str, Any]):
        print("\n" + "=" * 50)
        print("SPARK LOCATION ANALYSIS RESULTS SUMMARY")
        print("=" * 50)

        print(f"\nTotal contributors processed: {analysis['total_contributors']}")
        print(f"Successfully geocoded: {analysis['successful_locations']}")
        print(f"Unique countries: {analysis['unique_countries']}")
        print(f"Unique cities: {analysis['unique_cities']}")
        print(f"Average confidence: {analysis['avg_confidence']:.3f}")

        print(f"\nTop countries:")
        for country, count in analysis['top_countries'].items():
            percentage = (count / analysis['total_contributors']) * 100
            print(f"  {country:<20}: {count:>6} contributors ({percentage:>5.1f}%)")

    def stop(self):
        if hasattr(self, 'spark'):
            self.spark.stop()
            logger.info("Spark session stopped")


class LocationAnalyzer:
    def __init__(self, database_url: str, spark_master: str = None, n_partitions: int = 10):
        self.database_url = database_url
        self.spark_master = spark_master
        self.n_partitions = n_partitions
        self.classifier = None

    def analyze(self) -> Dict:
        try:
            logger.info(f"Initializing Spark location classifier with master: {self.spark_master}")
            self.classifier = LocationClassifier(
                database_url=self.database_url,
                spark_master=self.spark_master,
                n_partitions=self.n_partitions
            )

            return self.classifier.run_analysis()

        except Exception as e:
            logger.error(f"Error in SparkLocationAnalyzer: {e}")
            return {
                'error': str(e),
                'status': 'error'
            }

    def stop(self):
        if self.classifier:
            self.classifier.stop()