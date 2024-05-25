"""Store ETL module."""
from pyspark.sql import functions as F

from association.etl import find_raw_path, STORE_PATH
from association.utils import (
    log_time, read_csv, get_run_date_from_cli, LoggerMixin, delta_io
)

LOG = LoggerMixin().log


@log_time()
def etl(run_date: str) -> None:
    """etl code for store"""
    LOG.info(f"Running ETL for {run_date}.")

    path_to_read = find_raw_path(
        run_date, feed_name="/store.csv"
    )

    raw_store = read_csv(str(path_to_read), header=True, delimiter="|")
    LOG.info(f"Reading from {path_to_read}.")

    store_df = raw_store.select(
        F.col("store_code").cast("int").alias("store_id"),
        "store_description",
        F.col("area").cast("int"),
        "segment"
    )
    store_df = store_df.dropDuplicates(["store_id"])

    delta_io.write(
        df=store_df,
        path=STORE_PATH,
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
