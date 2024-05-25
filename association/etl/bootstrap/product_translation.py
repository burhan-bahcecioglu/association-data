"""Product Translation ETL module."""
from association.etl import (
    PRODUCT_PATH, PRODUCT_TRANSLATIONS_PATH, translate_from_uk_to_en
)
from association.utils import (
    log_time, LoggerMixin, get_run_date_from_cli, delta_io
)

LOG = LoggerMixin().log


@log_time()
def etl(run_date: str) -> None:
    """etl code for product translation"""
    LOG.info(f"Running ETL for {run_date}.")

    product_df = delta_io.read(path=PRODUCT_PATH)

    product_translation_df = product_df.select(
        "product_id",
        "product_description",
        translate_from_uk_to_en("product_description")
        .alias("product_description_en")
    )

    delta_io.write(
        df=product_translation_df,
        path=PRODUCT_TRANSLATIONS_PATH,
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
