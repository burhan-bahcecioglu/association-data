"""MongoDB result delivery module."""
from pyspark.sql import functions as F

from association.etl import STORE_TRANSLATIONS_PATH, STORE_PATH, PRODUCT_PATH
from association.modelling import (
    RULES_PATH as MODELLING_RULES_PATH,
    ITEM_SETS_PATH as MODELLING_ITEM_SETS_PATH
)
from association.result_delivery import RULES_PATH, ITEM_SETS_PATH
from association.utils import (
    log_time, get_run_date_from_cli,
    hard_cache, overwrite_csv, LoggerMixin, delta_io
)

LOG = LoggerMixin().log


@log_time()
def result_delivery(run_date: str) -> None:
    """result delivery code for mongodb"""
    LOG.info(f"Running result delivery for {run_date}.")

    rules_df = delta_io.read(path=MODELLING_RULES_PATH).select(
       "store_id", "antecedent", "consequent", "confidence", "lift", "support"
    )
    item_sets_df = delta_io.read(path=MODELLING_ITEM_SETS_PATH).select(
        "store_id", "items", "freq"
    )
    store_df = delta_io.read(path=STORE_PATH).select("store_id", "area")
    store_translations_df = delta_io.read(path=STORE_TRANSLATIONS_PATH).select(
        "store_id", "store_description_en", "segment_en"
    )
    product_translations_df = delta_io.read(path=PRODUCT_PATH).select(
        "product_id", "product_description_en"
    )

    rules_df = rules_df.join(store_df, on="store_id", how="inner")
    rules_df = rules_df.join(store_translations_df, on="store_id", how="inner")

    rules_df = rules_df.withColumn(
        "index", F.monotonically_increasing_id()
    )

    rules_df = rules_df.withColumn(
        "product_id", F.explode("consequent")
    ).join(
        product_translations_df, on="product_id", how="inner"
    ).withColumnRenamed(
        "product_description_en", "consequent"
    )

    rules_df = rules_df.withColumn(
        "product_id", F.explode("antecedent")
    ).join(
        product_translations_df, on="product_id", how="inner"
    ).withColumnRenamed(
        "product_description_en", "antecedent"
    )

    rules_df = rules_df.groupby(
        "index", "store_id", "store_description_en", "area", "segment_en"
    ).agg(
        F.collect_list("antecedent").alias("antecedent"),
        F.collect_list("consequent").alias("consequent"),
        F.avg("confidence").cast("double").alias("confidence"),
        F.avg("lift").cast("double").alias("lift"),
        F.avg("support").cast("double").alias("support")
    ).drop("index")

    item_sets_df = item_sets_df.withColumn(
        "index", F.monotonically_increasing_id()
    )

    item_sets_df = item_sets_df.withColumn(
        "product_id", F.explode("items")
    ).join(
        product_translations_df, on="product_id", how="inner"
    ).withColumnRenamed(
        "product_description_en", "item_sets_df"
    )

    item_sets_df = item_sets_df.groupby("index").agg(
        F.collect_list("items").alias("items"),
        F.avg("freq").cast("int").alias("freq")
    ).drop("index")

    rules_df = hard_cache(rules_df)
    item_sets_df = hard_cache(item_sets_df)

    overwrite_csv(df=rules_df, path=RULES_PATH, header=True, delimiter="|")

    overwrite_csv(
        df=item_sets_df, path=ITEM_SETS_PATH, header=True, delimiter="|"
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
