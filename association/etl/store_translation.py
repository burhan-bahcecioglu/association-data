"""Store Translation ETL module."""
from pyspark.sql import functions as F

from association.etl import (
    STORE_PATH, STORE_TRANSLATIONS_PATH, translate_from_uk_to_en
)
from association.utils import (
    log_time, get_run_date_from_cli, LoggerMixin, delta_io
)

LOG = LoggerMixin().log


@log_time()
def etl(run_date: str) -> None:
    """etl code for store translation"""
    LOG.info(f"Running ETL for {run_date}.")

    store_df = delta_io.read(path=STORE_PATH)

    store_translation_df = store_df.select(
        "store_id",
        "store_description",
        translate_from_uk_to_en("store_description")
        .alias("store_description_en"),
        "segment"
    )

    store_segment_df = store_df.select("segment").distinct()
    store_segment_df = store_segment_df.withColumn(
        "segment_en", translate_from_uk_to_en("segment")
    )

    store_translation_df = store_translation_df.join(
        F.broadcast(store_segment_df), on="segment", how="left"
    )

    store_translation_df = store_translation_df.select(
        "store_id",
        "store_description",
        "store_description_en",
        "segment",
        "segment_en"
    )

    delta_io.write(
        df=store_translation_df,
        path=STORE_TRANSLATIONS_PATH,
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
