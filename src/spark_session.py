from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException


def spark_session() -> SparkSession:

    if SparkSession.getActiveSession():
        return SparkSession.getActiveSession()

    else:
        spark: SparkSession = (
            SparkSession.builder.master("local")
            .appName("Airbnb")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.port", "4040")
            .getOrCreate()
        )
        return spark
