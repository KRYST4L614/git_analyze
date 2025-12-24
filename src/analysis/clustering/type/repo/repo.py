from pyspark.sql import DataFrame, functions as F
from src.analysis.spark.repo.repo import SparkBaseRepository


class AnalysisRepository(SparkBaseRepository):
    def __init__(self, spark_session, database_url: str, db_user: str = "postgres", db_password: str = "password"):
        super().__init__(spark_session, database_url, db_user, db_password)

    def load_repository_data(self, n_partitions: int = 10) -> DataFrame:
        query = """
            SELECT 
                id as repo_id,
                full_name,
                description,
                topics,
                owner_login,
                language,
                stargazers_count as stars,
                forks_count as forks
            FROM repositories
            WHERE description IS NOT NULL OR full_name IS NOT NULL
        """

        df = self.execute_query(query)
        return df.repartition(n_partitions)

    def save_clustering_results(self, results_df: DataFrame):
        save_df = results_df.select(
            F.col("repo_id"),
            F.col("full_name").alias("repo_name"),
            F.col("final_type"),
            F.col("corporate_weight"),
            F.col("educational_weight"),
            F.col("personal_weight"),
            F.col("confidence_score"),
            F.to_json(F.struct(
                F.col("topics"),
                F.col("owner_login"),
                F.col("org_name")
            )).alias("features")
        )

        self.write_table(save_df, "repository_clustering_results", "overwrite")