"""Association package"""
import logging
import os.path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

FILE_SCHEMA = "dbfs:/mnt/"
logging.info(f"File schema is {FILE_SCHEMA}.")
BUCKET_NAME = "datastore/association/"
logging.info(f"Bucket name is {BUCKET_NAME}.")
try:
    from pyspark.dbutils import DBUtils  # pylint: disable=E0611, E0401, C0412
except (ImportError, ModuleNotFoundError) as e:
    logging.error(
        f"Could not import PySpark dbutils. Do not use ETL Modules: \n {e}"
    )
    FILE_SCHEMA = "./"
    logging.info(f"Updated file schema is {FILE_SCHEMA}.")

    class DBUtils:  # pylint: disable= R0903
        """Dummy class to provide access to DBUtils class."""
        def __init__(self, spark_session: SparkSession):
            pass

logger = logging.getLogger('py4j')
logger.setLevel(logging.WARN)

builder = (
    SparkSession.builder
    .master('local[*]')
    .appName('term-project')
    .config('spark.default.parallelism', 8)
    .config('spark.sql.shuffle.partitions', 8)
    .config("spark.port.maxRetries", 32)
    .config('spark.sql.adaptive.enabled', False)
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '16g')
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

ROOT_PATH = os.path.join(FILE_SCHEMA, BUCKET_NAME)
if "spark" not in globals():
    spark = configure_spark_with_delta_pip(builder).getOrCreate()  # for local
else:
    spark = globals()["spark"]  # for remote (Databricks) execution
dbutils = DBUtils(spark)
