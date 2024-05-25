"""Product ETL module."""
from pyspark.sql import functions as F

from association.etl import (
    PRODUCT_TRANSLATIONS_PATH, find_raw_path, translate_from_uk_to_en
)
from association.utils import (
    log_time, read_csv, get_run_date_from_cli, LoggerMixin, delta_io
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
    LOG.info(f"Reading from {path_to_read}.")

    product_translation_df = delta_io.read(path=PRODUCT_TRANSLATIONS_PATH)

    product_df = raw_product.select(
        F.col("product_code").cast("int").alias("product_id"),
        "product_description"
    )

    product_df = product_df.dropDuplicates(["product_id"])

    product_translation_df = product_translation_df.select("product_id")

    delta_product_translation_df = product_translation_df.join(
        product_df, on="product_id", how="left_anti"
    )

    delta_product_translation_df = delta_product_translation_df.select(
        "product_id",
        "product_description",
        translate_from_uk_to_en("product_description")
        .alias("product_description_en")
    )

    delta_io.merge(
        source=delta_product_translation_df,
        target=PRODUCT_TRANSLATIONS_PATH,
        condition=["product_id"],
        when_matched="UPDATE_ALL",
        when_not_matched_insert="INSERT_ALL"
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
