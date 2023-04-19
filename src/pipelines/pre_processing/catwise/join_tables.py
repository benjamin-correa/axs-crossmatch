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


def _read_parquet(file_path: str):
    return spark.read.parquet(file_path)


def _unionAll(*sdf: DataFrame):
    return reduce(DataFrame.unionAll, sdf)


if __name__ == "__main__":
    spark = start_spark()

    intermediate_catwise_path = INTERMEDIATE_DATA_PATH.joinpath("catwise")
    primary_catwise_path = PRIMARY_DATA_PATH.joinpath("catwise")
    columns = DOWNLOAD_CATALOG_DICT["catwise"]["columns_map"].keys()
    try:
        os.mkdir(intermediate_catwise_path)
    except:
        log.info(f"Directory {intermediate_catwise_path} already exists")
        pass

    from pyspark.sql.functions import col
    files = os.listdir(intermediate_catwise_path)
    sdf_list = []
    total_tables = 0
    for file_name in files:
        if file_name.endswith(".parquet"):
            if len(sdf_list) > 200:
                log.info("Joining tables: %s/%s", total_tables, len(files))
                union_sdf = _unionAll(*sdf_list)
                union_sdf.persist(pyspark.StorageLevel.DISK_ONLY)
                sdf_list = [union_sdf]
                save_sdf = union_sdf.select([col(c).cast("string") for c in union_sdf.columns])
                save_sdf.write.mode("overwrite").option("header", False).csv(
                    str(primary_catwise_path.joinpath("catwise.csv"))
                )
                del save_sdf
            else:
                file_path = str(intermediate_catwise_path.joinpath(file_name))
                catwise_df = _read_parquet(file_path)
                catwise_df.createOrReplaceTempView("table")

                catwise_geom = spark.sql(
                    f"""
                    SELECT
                        ra AS ra_point, dec AS dec_point, {', '.join(columns)}
                    FROM
                        table
                    """
                )
                sdf_list.append(catwise_geom)
                total_tables += 1

    log.info("Joining tables: %s/%s", total_tables, len(files))
    union_sdf = _unionAll(*sdf_list)
    union_sdf.persist(pyspark.StorageLevel.DISK_ONLY)
    save_sdf = union_sdf.select([col(c).cast("string") for c in union_sdf.columns])
    save_sdf.write.mode("overwrite").option("header", False).csv(
        str(primary_catwise_path.joinpath("catwise.csv"))
    )
    del save_sdf
