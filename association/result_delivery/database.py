"""MongoDB result delivery module."""
from pyspark.sql import functions as F, Window as W

from association import DATABASE_URI
from association.etl import (
    STORE_TRANSLATIONS_PATH, STORE_PATH, PRODUCT_TRANSLATIONS_PATH
)
from association.modelling import RULES_PATH, ITEM_SETS_PATH
from association.result_delivery import (
    DATABASE, RULES_COLLECTION, ITEM_SETS_COLLECTION
)
from association.utils import (
    LoggerMixin, log_time, hard_cache,
    get_run_date_from_cli, overwrite_mongo_db, delta_io
)

LOG = LoggerMixin().log


@log_time()
def result_delivery(run_date: str) -> None:
    """result delivery code for mongodb"""
    LOG.info(f"Running result delivery for {run_date}.")

    rules_df = delta_io.read(path=RULES_PATH).select(
       "store_id", "antecedent", "consequent", "confidence", "lift", "support"
    )

    item_sets_df = delta_io.read(path=ITEM_SETS_PATH).select(
        "store_id", "items", "freq"
    )

    store_df = delta_io.read(path=STORE_PATH).select("store_id", "area")
    store_translations_df = delta_io.read(path=STORE_TRANSLATIONS_PATH).select(
        "store_id", F.lit("store").alias("store_description_en"), "segment_en"
    )
    product_translations_df = delta_io.read(
        path=PRODUCT_TRANSLATIONS_PATH
    ).select("product_id", "product_description_en")

    rules_df = rules_df.join(
        store_df, on="store_id", how="inner"
    ).join(
        store_translations_df, on="store_id", how="inner"
    ).withColumn(
        "store_id", F.dense_rank().over(W.orderBy(F.desc("store_id")))
    )

    rules_df = rules_df.withColumn(
        "index", F.monotonically_increasing_id()
    )

    rules_df = rules_df.withColumn(
        "product_id", F.explode("consequent")
    ).join(
        product_translations_df, on="product_id", how="inner"
    ).drop("consequent").withColumnRenamed(
        "product_description_en", "consequent"
    )

    rules_df = rules_df.withColumn(
        "product_id", F.explode("antecedent")
    ).join(
        product_translations_df, on="product_id", how="inner"
    ).drop("antecedent").withColumnRenamed(
        "product_description_en", "antecedent"
    )

    rules_df = rules_df.groupby(
        "index", "store_id", "store_description_en", "area", "segment_en"
    ).agg(
        F.collect_list("antecedent").alias("antecedent"),
        F.collect_set("consequent").alias("consequent"),
        F.avg("confidence").cast("double").alias("confidence"),
        F.avg("lift").cast("double").alias("lift"),
        F.avg("support").cast("double").alias("support")
    ).drop("index")

    item_sets_df = item_sets_df.join(
        store_df, on="store_id", how="inner"
    ).join(
        store_translations_df, on="store_id", how="inner"
    ).withColumn(
        "store_id", F.dense_rank().over(W.orderBy(F.desc("store_id")))
    )

    item_sets_df = item_sets_df.withColumn(
        "index", F.monotonically_increasing_id()
    )

    item_sets_df = item_sets_df.withColumn(
        "product_id", F.explode("items")
    ).join(
        product_translations_df, on="product_id", how="inner"
    ).drop("items")

    item_sets_df = item_sets_df.groupby(
        "index", "store_id", "store_description_en", "area", "segment_en"
    ).agg(
        F.collect_list("product_description_en").cast("string").alias("items"),
        F.avg("freq").cast("int").alias("freq")
    ).drop("index")

    rules_df = hard_cache(rules_df)
    item_sets_df = hard_cache(item_sets_df)

    overwrite_mongo_db(
        df=rules_df,
        database_uri=DATABASE_URI,
        database=DATABASE, collection=RULES_COLLECTION
    )

    overwrite_mongo_db(
        df=item_sets_df,
        database_uri=DATABASE_URI,
        database=DATABASE, collection=ITEM_SETS_COLLECTION
    )


def main() -> None:
    """
    run result_delivery function.
    """
    arguments = get_run_date_from_cli()
    run_date = arguments.run_date
    result_delivery(run_date)


if __name__ == "__main__":
    main()
