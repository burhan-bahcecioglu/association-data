"""Transactional Sales ETL module."""
from pyspark.errors import AnalysisException, IllegalArgumentException
from pyspark.sql import functions as F

from association.etl import (
    find_raw_path, TRANSACTIONAL_SALES_PATH
)
from association.utils import (
    log_time, date_add_str, read_csv,
    get_run_date_from_cli, LoggerMixin, delta_io
)

LOG = LoggerMixin().log


@log_time()
def etl(run_date: str) -> None:
    """etl code for transactional_sales"""
    LOG.info(f"Running ETL for {run_date}.")

    path_to_read = find_raw_path(
        run_date, feed_name="transactional_sales.csv"
    )

    raw_transactional_sales = read_csv(
        str(path_to_read), header=True, delimiter="|"
    )
    LOG.info(f"Reading from {path_to_read}.")

    transactional_sales_df = raw_transactional_sales.select(
        F.col("sales_date").cast("date").alias("date"),
        F.col("transaction_Id").cast("int").alias("transaction_id"),
        F.col("sales_id").cast("int"),
        F.col("customer_code").cast("int").alias("customer_id"),
        F.col('store_code').cast("int").alias("store_id"),
        F.col("product_code").cast("int").alias("product_id"),
        F.col("sales_type").cast("int"),
        "sales_channel",
        F.col("sales_quantity").cast("float"),
        F.col("sales_revenue").cast("float"),
        F.col("tax_amount").cast("float"),
        F.col("discount").cast("float"),
        F.col("promo_code").cast("int").alias("promo_id"),
    )
    transactional_sales_df = transactional_sales_df.dropDuplicates(
        ["transaction_id"]
    )

    min_date, max_date = tuple(
        transactional_sales_df.agg(
            F.min("date"), F.max("date")
        ).collect()[0]
    )
    partition_cols = ["date"]
    filter_expr = f"(date>='{min_date}' AND date<='{max_date}')"

    delta_io.write_partitioned(
        df=transactional_sales_df,
        path=TRANSACTIONAL_SALES_PATH,
        mode="overwrite",
        partition_columns=partition_cols,
        replace_where=filter_expr
    )


def main() -> None:
    """
    for each iteration increments a day
      - reads raw transactional_sales
      - transforms raw transactional_sales
      - saves transformed transactional_sales as ready to use
    """
    arguments = get_run_date_from_cli()
    run_date = arguments.run_date
    run_date = date_add_str(run_date, **{"days": -10})
    while True:
        try:
            etl(run_date)
            run_date = date_add_str(run_date, **{"days": 1})
        except (AnalysisException, IllegalArgumentException) as e:
            LOG.error(f"Stops execution due to error: \n {e}")
            break


if __name__ == "__main__":
    main()
