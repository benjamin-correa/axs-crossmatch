import logging
import os
import sys
from functools import reduce

from pyspark.sql import DataFrame
import pyspark

sys.path.append("src")

from utils.global_constants import (
    DOWNLOAD_CATALOG_DICT,
    INTERMEDIATE_DATA_PATH,
    PRIMARY_DATA_PATH,
)  # noqa E402

from utils.global_utils import start_spark, transform_coordinates  # noqa E402

log = logging.getLogger(__name__)


def _read_csv(file_path: str):
    return spark.read.csv(file_path)


def _unionAll(*sdf: DataFrame):
    return reduce(DataFrame.unionAll, sdf)


if __name__ == "__main__":
    spark = start_spark()

    file_path = PRIMARY_DATA_PATH + "catwise.csv"
    primary_catwise_path = PRIMARY_DATA_PATH + "catwise/"
    columns = DOWNLOAD_CATALOG_DICT["catwise"]["columns_map"].keys()
    from pyspark.sql.functions import col

    file_path = str(file_path)
    catwise_df = _read_csv(file_path)
    catwise_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    catwise_df.createOrReplaceTempView("table")
    catwise_df = spark.sql(
        f"""
        SELECT
            _c2 AS ra_point, _c3 AS dec_point, {', '.join(catwise_df.columns)}
        FROM
            table
        """
    )
    catwise_df = catwise_df.select([col(c).cast("string") for c in catwise_df.columns])
    catwise_df.write.mode("overwrite").option("header", False).csv(
        str(primary_catwise_path + "catwise.csv")
    )

