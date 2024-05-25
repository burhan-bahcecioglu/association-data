"""Product ETL module."""
from pyspark.sql import functions as F

from association.etl import find_raw_path, PRODUCT_PATH
from association.utils import (
    log_time, get_run_date_from_cli, read_csv, LoggerMixin, delta_io
)

LOG = LoggerMixin().log


@log_time()
def etl(run_date: str) -> None:
    """etl code for product"""
    LOG.info(f"Running ETL for {run_date}.")

    path_to_read = find_raw_path(
        run_date, feed_name="product.csv"
    )

    raw_product = read_csv(str(path_to_read), header=True, delimiter="|")

    product_df = raw_product.select(
        F.col("product_code").cast("int").alias("product_id"),
        "product_description",
        F.col("product_group_1_code").alias("product_hierarchy_1_code"),
        F.col("product_group_2_code").alias("product_hierarchy_2_code"),
        F.col("product_group_3_code").alias("product_hierarchy_3_code"),
        F.col("product_group_4_code").alias("product_hierarchy_4_code"),
        F.col("product_group_5_code").alias("product_hierarchy_5_code"),
        F.col("product_group_6_code").alias("product_hierarchy_6_code"),
        F.col("product_group_1_code")
        .alias("product_hierarchy_1_description"),
        F.col("product_group_2_code")
        .alias("product_hierarchy_2_description"),
        F.col("product_group_3_code")
        .alias("product_hierarchy_3_description"),
        F.col("product_group_4_code")
        .alias("product_hierarchy_4_description"),
        F.col("product_group_5_code")
        .alias("product_hierarchy_5_description"),
        F.col("product_group_6_code")
        .alias("product_hierarchy_6_description"),
        F.coalesce(
            "product_group_6_code", "product_group_5_code",
            "product_group_4_code", "product_group_3_code",
            "product_group_2_code", "product_group_1_code"
        ).alias("last_product_hierarchy_code"),
        F.coalesce(
            "product_group_6_code", "product_group_5_code",
            "product_group_4_code", "product_group_3_code",
            "product_group_2_code", "product_group_1_code"
        ).alias("last_product_hierarchy_description"),
    )

    product_df = product_df.dropDuplicates(["product_id"])

    delta_io.write(
        df=product_df,
        path=PRODUCT_PATH,
        mode="overwrite"
    )


def main() -> None:
    """
    run etl function.
    """
    arguments = get_run_date_from_cli()
    run_date = arguments.run_date
    etl(run_date)


if __name__ == "__main__":
    main()
