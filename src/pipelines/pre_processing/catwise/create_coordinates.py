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
    total_tables = 0
    for file_name in files:
        if file_name.endswith(".csv"):
            file_path = str(intermediate_catwise_path + file_name)
            catwise_df = _read_csv(file_path)
            fix_table = spark.read.option("header", True).csv("data/03_primary/catwise2020_table1_20201012.csv")
            fix_table.createOrReplaceTempView("offset_fix")
            catwise_df.createOrReplaceTempView("table")
            columns = catwise_df.columns
            sub_statement = {
                "_c2": "_c2 + offsetra AS _c2",
                "_c3": "_c3 + offsetdec AS _c3",
                "_c12": "_c12 + offsetpmra AS _c12",
                "_c13": "_c13 + offsetpmdec AS _c13",
            }
            columns = [sub_statement.get(item,item) for item in columns]
            catwise_geom = spark.sql(
                f"""
                SELECT
                    _c2 + offsetra AS ra_point, _c3 + offsetdec AS dec_point, {', '.join(columns)}
                FROM
                    table, offset_fix
                WHERE
                    offset_fix.tile = '{file_name.split("_")[0]}'
                """
            )
            total_tables += 1
            log.info("Adding table %s: %s/%s", file_name.replace("csv", "parquet"), total_tables, len(files))
            catwise_geom.write.mode("overwrite").parquet(
                str(primary_catwise_path + "catwise/" + file_name.replace("csv", "parquet"))
            )

