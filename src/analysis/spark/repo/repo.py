from pyspark.sql import DataFrame
from typing import Dict, Any


class SparkBaseRepository:
    def __init__(self, spark_session, database_url: str, db_user: str = "postgres", db_password: str = "password"):
        self.spark = spark_session
        self.database_url = database_url
        self.db_user = db_user
        self.db_password = db_password

    def _get_jdbc_options(self, table_name: str = None) -> Dict[str, str]:
        options = {
            "url": self.database_url,
            "user": self.db_user,
            "password": self.db_password,
            "driver": "org.postgresql.Driver"
        }
        if table_name:
            options["dbtable"] = table_name
        return options

    def read_table(self, table_name: str, columns: list = None) -> DataFrame:
        df = self.spark.read \
            .format("jdbc") \
            .options(**self._get_jdbc_options(table_name)) \
            .load()

        if columns:
            df = df.select(*columns)

        return df

    def execute_query(self, query: str) -> DataFrame:
        return self.spark.read \
            .format("jdbc") \
            .options(**{
            **self._get_jdbc_options(),
            "query": query
        }) \
            .load()

    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        df.write \
            .format("jdbc") \
            .options(**self._get_jdbc_options(table_name)) \
            .mode(mode) \
            .save()
