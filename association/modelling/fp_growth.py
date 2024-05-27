"""FP growth model module"""
import argparse
import os.path
from datetime import date, timedelta

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import functions as F

from association.etl import TRANSACTIONAL_SALES_PATH
from association.modelling import (
    LOOK_BACK_PERIOD, RULES_PATH, ITEM_SETS_PATH,
    MODEL_DATA_PATH, PRODUCTS_TO_FILTER_OUT_PATH
)
from association.utils import (
    log_time, date_add_str, hard_cache,
    get_run_date_from_cli, LoggerMixin, delta_io
)

LOG = LoggerMixin().log


def get_arguments() -> argparse.Namespace:
    """
    Parse command line arguments for etl modules (options)

    :return: command line arguments
    :rtype: argparse.Namespace
    """
    # create parser
    parser = argparse.ArgumentParser(description=__doc__)

    # add arguments to the parser
    parser.add_argument(
        "-rd", "--run-date", type=str,
        default=str(date.today() - timedelta(days=1)),
        help="run date in YYYY-MM-DD style, default: today - 1"
    )
    parser.add_argument(
        "-ms", "--min-support", type=float,
        default=0.01,
        help="min support between (0, 1)"
    )
    parser.add_argument(
        "-mc", "--min-confidence", type=float,
        default=0.01,
        help="min confidence between (0, 1)"
    )

    # parse arguments
    arguments, unknown = parser.parse_known_args()
    if unknown:
        LOG.warning(f"unknown arguments: {unknown}")
    LOG.info(f"arguments: {arguments}")
    # return only known arguments, unknown arguments will be discarded
    return arguments


@log_time()
def model(run_date: str, min_support: float, min_confidence: float) -> None:
    """modelling code for fp growth algorithm"""
    LOG.info(f"Running fp_growth for {run_date}.")

    sales_df = delta_io.read(path=TRANSACTIONAL_SALES_PATH)
    products_to_filter_out_df = delta_io.read(path=PRODUCTS_TO_FILTER_OUT_PATH)

    stats = sales_df.agg(
        F.countDistinct("date").alias("date_count"),
        F.countDistinct("sales_id").alias("basket_count"),
        F.count("transaction_id").alias("transaction_count")
    ).withColumn(
        "transaction_per_basket",
        F.col("transaction_count") / F.col("basket_count")
    ).collect()

    date_count = stats[0]["date_count"]
    basket_count = stats[0]["basket_count"]
    transaction_count = stats[0]["transaction_count"]
    transaction_per_basket = stats[0]["transaction_per_basket"]

    LOG.info(f"Total date count: {date_count:,}")
    LOG.info(f"Total basket count: {basket_count:,}")
    LOG.info(f"Total transaction count: {transaction_count:,}")
    LOG.info(f"Total transaction per basket: {transaction_per_basket:.2f}")

    # filter out some rows
    sales_df = sales_df.join(
        products_to_filter_out_df, on="product_id", how="left_anti"
    )

    sales_df = sales_df.where(
        F.col("date").between(
            lowerBound=date_add_str(run_date, **{"days": -LOOK_BACK_PERIOD}),
            upperBound=run_date
        )
    )

    df = sales_df.groupBy("store_id", "sales_id").agg(
        F.collect_set(F.col("product_id")).alias("basket")
    )

    df = hard_cache(df.repartition("store_id"), partition_by=["store_id"])

    stores = df.select("store_id").distinct(
    ).rdd.flatMap(lambda x: x).collect()

    len_stores = len(stores)
    LOG.info(f"# of stores to model is {len_stores:,}.")

    fp_modeler = FPGrowth(
        minSupport=min_support, minConfidence=min_confidence,
        itemsCol='basket', predictionCol='prediction'
    )

    counter = 0
    for store in stores:
        replace_where = f"store_id=={store}"
        df_to_model = df.where(replace_where)
        LOG.info(
            f"Running fp_growth for {store}. Status: {counter}/{len_stores}"
        )
        fp_model = fp_modeler.fit(df_to_model)

        delta_io.write_partitioned(
            df=fp_model.associationRules.withColumn("store_id", F.lit(store)),
            path=RULES_PATH,
            partition_columns=["store_id"],
            mode="overwrite",
            replace_where=replace_where,
        )

        delta_io.write_partitioned(
            df=fp_model.freqItemsets.withColumn("store_id", F.lit(store)),
            path=ITEM_SETS_PATH,
            partition_columns=["store_id"],
            mode="overwrite",
            replace_where=replace_where,
        )

        fp_model.save(os.path.join(MODEL_DATA_PATH, replace_where))

        counter += 1


def main() -> None:
    """
    for each iteration increments a day
      - reads raw transactional_sales
      - transforms raw transactional_sales
      - saves transformed transactional_sales as ready to use
    """
    run_date = get_run_date_from_cli().run_date
    arguments = get_arguments()
    min_support = arguments.min_support
    min_confidence = arguments.min_confidence
    model(
        run_date=run_date,
        min_support=min_support,
        min_confidence=min_confidence
    )


if __name__ == "__main__":
    main()
