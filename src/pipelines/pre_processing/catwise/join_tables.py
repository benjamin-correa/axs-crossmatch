import logging
import os
import sys
from functools import reduce

from tqdm import tqdm
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

    intermediate_catwise_path = INTERMEDIATE_DATA_PATH + "catwise/"
    primary_catwise_path = PRIMARY_DATA_PATH + "catwise/"
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
    batch_tables = 50
    union_sdf = None
    for file_name in files:
        if file_name.endswith(".csv"):
            if len(sdf_list) < batch_tables:
                file_path = str(intermediate_catwise_path + file_name)
                catwise_df = _read_csv(file_path)
                catwise_df.createOrReplaceTempView("table")

                catwise_geom = spark.sql(
                    f"""
                    SELECT
                        _c2 AS ra_point, _c3 AS dec_point, {', '.join(catwise_df.columns)}
                    FROM
                        table
                    """
                )
                sdf_list.append(catwise_geom)
                total_tables += 1
                log.info("Adding table %s: %s/%s", file_name, total_tables, len(files))
            if len(sdf_list) >= batch_tables:
                log.info("Joining tables: %s/%s", total_tables, len(files))
                if not union_sdf:
                    union_sdf = sdf_list.pop(0)
                # union_sdf = _unionAll(*sdf_list)
                for sdf in sdf_list:
                    union_sdf = union_sdf.union(sdf)
                    sdf.unpersist()
                union_sdf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
                union_sdf.repartition(total_tables*2)
                sdf_list = [union_sdf]
                save_sdf = union_sdf.select([col(c).cast("string") for c in union_sdf.columns])
                save_sdf.write.mode("overwrite").option("header", False).csv(
                    str(primary_catwise_path + "catwise.csv")
                )
                save_sdf.unpersist()

    log.info("Joining tables: %s/%s", total_tables, len(files))
    # union_sdf = _unionAll(*sdf_list)
    if not union_sdf:
        union_sdf = sdf_list.pop(0)
    for sdf in sdf_list:
        union_sdf = union_sdf.union(sdf)
        sdf.unpersist()
    union_sdf.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    save_sdf = union_sdf.select([col(c).cast("string") for c in union_sdf.columns])
    save_sdf.write.mode("overwrite").option("header", False).csv(
        str(primary_catwise_path + "catwise.csv")
    )
    del save_sdf
