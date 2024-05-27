"""IO related utility."""
import os
from typing import Dict, Any, Union, List, Optional
from uuid import uuid4

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from association import spark, ROOT_PATH
from association.utils.log import LoggerMixin


LOG = LoggerMixin().log


def hard_cache(df: DataFrame, **kwargs) -> DataFrame:
    """writes & read df from object storage"""
    LOG.info(
        f"Caching data from with options {kwargs}."
    )
    partition_by = kwargs.pop("partition_by", [])
    path = os.path.join(ROOT_PATH, "cache", str(uuid4()))
    LOG.info(f"Writing df to {path}.")
    file_format = kwargs.pop("file_format", "parquet")
    df.write.partitionBy(
        partition_by
    ).format(
        file_format
    ).options(
        **kwargs
    ).save(path)
    LOG.info(f"Reading df from {path}.")
    return spark.read.format(file_format).load(path)


def read_csv(path: str, **kwargs) -> DataFrame:
    """reads csv from object storage"""
    LOG.info(
        f"Reading data from {path}. \n"
        f"with options {kwargs}."
    )
    return spark.read.format("csv").options(**kwargs).load(path)


def overwrite_csv(df: DataFrame, path: str, **kwargs) -> None:
    """overwrites to object storage as csv"""
    LOG.info(
        f"Writing data to {path}. \n"
        f"with options {kwargs}."
    )

    df.repartition(1).write.format("csv").options(**kwargs).save(
        path, mode="overwrite"
    )


def overwrite_mongo_db(
        df: DataFrame,
        database_uri: str,
        database: str,
        collection: str, **kwargs
) -> None:
    """overwrites to mongo db"""
    LOG.info(
        f"Writing data to {database}*-*{collection}. \n"
        f"with options {kwargs}."
    )

    df.write.format(
        "mongodb"
    ).mode(
        "overwrite"
    ).option(
        "database.uri", database_uri
    ).options(
        database=database, collection=collection, **kwargs
    ).save()


class DeltaIOAdapter(LoggerMixin):
    """Generic class for all delta IO operations"""

    def __init__(self, spark_session: SparkSession = spark):
        """
        Initializes the adapter with common attributes
        """
        super().__init__()
        self.spark = spark_session
        self.log.info("Delta adapter is initialized.")

    def read(
            self, path: str, spark_options: Dict[str, Any] = None, **kwargs
    ) -> DataFrame:
        """
        Read delta data with given parameters.
        """
        spark_options = spark_options or {}

        self.log.info(f"Reading delta data from {path}")
        df = self.spark.read.load(
            path=path, format="delta", **spark_options)

        partition_filter = kwargs.pop("partition_filter", None)
        if partition_filter:
            partition_filter = [
                " ".join(list(partition)) for partition in partition_filter
            ]
            df = df.filter(" AND ".join(partition_filter))

        return df

    def write(
            self,
            df: DataFrame,
            path: str,
            mode: str,
            partition_columns: Optional[Union[List[str], str]] = None,
            repartition: Optional[Union[List[str], str, int]] = None,
            spark_options: Dict[str, Any] = None,
            **kwargs
    ) -> None:
        """
        Write given :class:`DataFrame` to given path with the given mode.
        """
        spark_options = spark_options or {}

        overwrite_schema = os.environ.get(
            "DELTA_OVERWRITE_SCHEMA_ALLOWED", None
        )
        if (
                isinstance(overwrite_schema, str) and
                (overwrite_schema.upper() == "TRUE")
        ):
            spark_options["overwriteSchema"] = spark_options.get(
                "overwriteSchema", True
            )
        elif overwrite_schema is not None:
            self.log.warning(
                "Unexpected value set to environment variable "
                "`DELTA_OVERWRITE_SCHEMA_ALLOWED`. Expected `TRUE` or nothing."
                f" Received `{overwrite_schema}`"
            )

        delta_optimize_write_flag = kwargs.pop(
            "delta_optimize_write_flag", None
        )
        is_optimize_write = self.spark.conf.get(
            "spark.databricks.delta.optimizeWrite.enabled", None
        )

        if delta_optimize_write_flag is not None:
            flag_value = str(delta_optimize_write_flag).lower()
            self.spark.conf.set(
                "spark.databricks.delta.optimizeWrite.enabled",
                flag_value
            )
            self.log.info(
                "Set spark.databricks.delta.optimizeWrite.enabled to %s "
                "since delta_optimize_write_flag is given as %s "
                "for destination path: %s",
                flag_value,
                delta_optimize_write_flag,
                path
            )
        elif str(is_optimize_write).lower() == "true":
            # it can be given as "true" or True
            repartition = None
            self.log.info(
                "Disabled repartitioning since optimized write is enabled "
                "for destination path: %s",
                path
            )

        if repartition:
            if isinstance(repartition, list):
                df = df.repartition(*repartition)
            else:
                df = df.repartition(repartition)

        self.log.info(f"Writing delta file to {path} with {mode} mode")
        df.write.save(
            path=path,
            mode=mode,
            format="delta",
            partitionBy=partition_columns,
            **spark_options
        )

        if (
                delta_optimize_write_flag is not None and
                is_optimize_write is not None
        ):
            self.log.info(
                "Set spark.databricks.delta.optimizeWrite.enabled back to "
                "its original value: %s after temporarily changing since "
                "delta_optimize_write_flag is given as %s",
                str(is_optimize_write).lower(),
                delta_optimize_write_flag
            )
            self.spark.conf.set(
                "spark.databricks.delta.optimizeWrite.enabled",
                str(is_optimize_write).lower()
            )

    def write_partitioned(
            self,
            df: DataFrame,
            path: str,
            mode: str,
            replace_where: Optional[str] = None,
            partition_columns: Optional[Union[List[str], str]] = None,
            repartition: Optional[Union[List[str], str, int]] = None,
            spark_options: Dict[str, Any] = None,
            **kwargs
    ) -> None:
        """
        Write given partitions of a :class:`DataFrame` to given path.
        """
        spark_options = spark_options or {}
        assert isinstance(replace_where, str), (
            "replace_where should be a string. it is given as: {}"
        ).format(type(replace_where))

        self.log.info(
            "Writing to %s with %s mode and replaceWhere condition: %s",
            path, mode, replace_where
        )

        # this is implemented because we cannot be sure what we will receive
        # from spark_options dict. so we should merge in order to avoid
        # conflict in arguments
        spark_options = spark_options or {}
        spark_options.update({
            "replaceWhere": replace_where,
        })
        self.write(
            df=df,
            path=path,
            mode=mode,
            partition_columns=partition_columns,
            repartition=repartition,
            spark_options=spark_options,
            **kwargs
        )

    def merge(
            self,
            source: DataFrame,
            target: str,
            condition: Union[str, List[str]],
            when_matched: Optional[Union[dict, str]] = None,
            when_not_matched_insert: Optional[Union[dict, str]] = None,
    ) -> None:
        """
        Merge the given :class:`DataFrame` with the given path.
        """
        condition_expr = condition if not isinstance(condition, list) else ""
        if not condition_expr:
            for column in condition:
                condition_expr += f"target.{column} = source.{column} AND "
            condition_expr = condition_expr.rsplit("AND ", 1)[0]

        if not DeltaTable.isDeltaTable(self.spark, target):
            raise Exception("Table does not exist.")

        delta_table = DeltaTable.forPath(sparkSession=self.spark, path=target)
        self.log.info(
            f"Merging the delta file from {target} with the source df"
        )

        merge_expression = delta_table.alias("target").merge(
            source.alias("source"), condition_expr)
        if when_matched == "UPDATE_ALL":
            merge_expression = merge_expression.whenMatchedUpdateAll()
        elif when_matched == "DELETE":
            merge_expression = merge_expression.whenMatchedDelete()
        elif when_matched:
            merge_expression = merge_expression.whenMatchedUpdate(
                set=when_matched
            )

        if when_not_matched_insert == "INSERT_ALL":
            merge_expression = merge_expression.whenNotMatchedInsertAll()
        elif when_not_matched_insert:
            merge_expression = merge_expression.whenNotMatchedInsert(
                values=when_not_matched_insert)

        if when_matched or when_not_matched_insert:
            merge_expression.execute()
        else:
            self.log.warning(
                "Merging operation is skipped since the both when_matched and "
                "when_not_matched_insert parameters aren't provided"
            )
