from pyspark.sql import DataFrame, functions as F
from src.analysis.spark.repo.repo import SparkBaseRepository


class LocationRepository(SparkBaseRepository):
    def __init__(self, spark_session, database_url: str, db_user: str = "postgres", db_password: str = "password"):
        super().__init__(spark_session, database_url, db_user, db_password)

    def load_contributor_location_data(self, n_partitions: int = 10) -> DataFrame:
        query = """
            SELECT 
                c.id as contributor_id,
                c.login as contributor_login,
                c.location,
                c.company,
                c.email,
                COUNT(DISTINCT cm.repo_id) as repo_count,
                COUNT(cm.sha) as commit_count,
                COUNT(DISTINCT DATE(cm.author_date)) as active_days
            FROM contributors c
            LEFT JOIN commits cm ON c.id = cm.author_id
            WHERE c.location IS NOT NULL 
                AND c.location != ''
            GROUP BY c.id, c.login, c.location, c.company, c.email
            HAVING COUNT(cm.sha) >= 1
        """

        df = self.execute_query(query)
        return df.repartition(n_partitions)

    def save_contributor_locations(self, results_df: DataFrame):
        save_df = results_df.select(
            F.col("contributor_id"),
            F.col("original_location"),
            F.col("country_name"),
            F.col("city_name"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("confidence")
        )

        self.write_table(save_df, "contributor_locations", "overwrite")

    def save_countries(self, countries_df: DataFrame):
        countries_df.write \
            .format("jdbc") \
            .options(**self._get_jdbc_options("countries")) \
            .mode("append") \
            .save()

    def save_cities(self, cities_df: DataFrame):
        cities_df.write \
            .format("jdbc") \
            .options(**self._get_jdbc_options("cities")) \
            .mode("append") \
            .save()