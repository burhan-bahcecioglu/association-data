"""Association package"""
import logging
import os.path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(name)s:%(filename)s:%(lineno)s}"
           " %(levelname)s - %(message)s",
)
logger = logging.getLogger('py4j')
logger.setLevel(logging.WARNING)
LOG = logging.getLogger("association")

FILE_SCHEMA = "dbfs:/mnt/"
LOG.info(f"File schema is {FILE_SCHEMA}.")
BUCKET_NAME = "datastore/association/"
LOG.info(f"Bucket name is {BUCKET_NAME}.")
DATABASE_URI = os.getenv("DATABASE_URI")
try:
    from pyspark.dbutils import DBUtils  # pylint: disable=E0611, E0401, C0412
except (ImportError, ModuleNotFoundError) as e:
    LOG.error(
        f"Could not import PySpark dbutils. Do not use ETL Modules: \n {e}"
    )
    FILE_SCHEMA = "/home/burhan"
    LOG.info(f"Updated file schema is {FILE_SCHEMA}.")

    class DBUtils:  # pylint: disable= R0903
        """Dummy class to provide access to DBUtils class."""
        def __init__(self, spark_session: SparkSession):
            pass


ROOT_PATH = os.path.join(FILE_SCHEMA, BUCKET_NAME)
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
    .config("spark.mongodb.read.connection.uri", DATABASE_URI)
    .config("spark.mongodb.write.connection.uri", DATABASE_URI)
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
    )
)

if "sc" not in globals():
    spark = configure_spark_with_delta_pip(builder).getOrCreate()  # for local
    LOG.info("Configured spark for local.")
else:
    spark = globals()["spark"]  # for remote (Databricks) execution
    LOG.info("Configured spark for databricks.")
dbutils = DBUtils(spark)
