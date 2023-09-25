import logging
import os
import sys
from typing import List
from typing import Optional

from pyspark.sql.types import DecimalType, DoubleType, StringType
from pyspark.sql.functions import col

sys.path.append("src")

from utils.global_constants import (
    DOWNLOAD_CATALOG_DICT,
    RAW_DATA_PATH,
    INTERMEDIATE_DATA_PATH,
)  # noqa E402

from utils.global_utils import start_spark  # noqa E402

log = logging.getLogger(__name__)
def _read_catwise_tbl_gz(file_path: str):
    """Read and parse a catwise .tbl.gz.

    Args:
        file_path (str): Path of the file

    Returns:
        pd.DataFrame: Parsed .tbl.gz
    """
    catalog_data = spark.read.text(str(file_path))

    # Getting the column names
    rdd_without_comments = catalog_data.filter(
        ((~catalog_data.value.startswith("|")) & (~catalog_data.value.startswith("\\")))
    )
    # rdd_header = rdd.filter(lambda x: (x.startswith("|")))
    rdd_header = catalog_data.filter(catalog_data.value.startswith("|"))

    # Getting the column names
    table_columns = rdd_header.take(1)[0].value.lower().replace(" ", "").split("|")

    # Filtering empty list values
    table_columns = table_columns[1 : len(table_columns) - 1]

    # Splitting the rows by " "
    rdd_data = rdd_without_comments.rdd.map(lambda x: x.value.split())

    # Creating final table
    final_table = spark.createDataFrame(data=rdd_data, schema=table_columns)

    return final_table
if __name__ == "__main__":
    spark = start_spark()

    log.info(
        "Processing: data/02_intermediate/CatWISE2020_Table1_20201012.tbl"
    )

    df = _read_catwise_tbl_gz("data/02_intermediate/CatWISE2020_Table1_20201012.tbl")

    # Casting DataTypes and selecting columns
    df = df.select(
        col("tile").cast(StringType()),
        col("offsetra_ta").cast(DoubleType()),
        col("offsetra").cast(DoubleType()),
        col("offsetdec").cast(DoubleType()),
        col("offsetpmra").cast(DecimalType(15,5)),
        col("offsetpmdec").cast(DecimalType(15,5)),
    )
    df.toPandas().to_csv("data/03_primary/catwise2020_table1_20201012.csv", index=False)


    