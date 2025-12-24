import os
from pyspark.sql import SparkSession


class SparkConfig:
    @staticmethod
    def get_spark_session(
            app_name: str = "GitHubRepositoryClassifier",
            spark_master: str = None,
            n_partitions: int = 10
    ) -> SparkSession:

        spark_master = spark_master or os.getenv("SPARK_MASTER", "spark://spark-master:7077")

        builder = SparkSession.builder \
            .appName(app_name) \
            .master(spark_master) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.executor.memory", "1g") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", str(n_partitions)) \
            .config("spark.driver.host", "spark-client") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.driver.port", "7001") \
            .config("spark.driver.blockManager.port", "7002") \
            .config("spark.ui.port", "4040") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.cores", "2") \
            .config("spark.network.timeout", "300s") \
            .config("spark.executor.heartbeatInterval", "30s") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .config("spark.local.dir", "/tmp/spark-local") \
            .config("spark.pyspark.python", "./environment/bin/python") \
            .config("spark.pyspark.driver.python", "./environment/bin/python") \
            .config("spark.driver.extraJavaOptions",
                    "-Djava.net.preferIPv4Stack=true "
                    "-Djava.net.preferIPv4Addresses=true "
                    "-Djava.security.egd=file:/dev/./urandom") \
            .config("spark.executor.extraJavaOptions",
                    "-Djava.net.preferIPv4Stack=true "
                    "-Djava.net.preferIPv4Addresses=true "
                    "-Djava.security.egd=file:/dev/./urandom")

        postgres_jar = os.getenv("SPARK_POSTGRES_JAR", "/opt/spark/jars/postgresql-42.6.0.jar")
        if postgres_jar:
            builder = builder \
                .config("spark.jars", postgres_jar) \
                .config("spark.driver.extraClassPath", postgres_jar) \
                .config("spark.executor.extraClassPath", postgres_jar)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark