import logging
import os
import sys
from typing import List
from typing import Optional

import pyspark.sql.types
from pyspark.sql.functions import col
import os

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


def _cast_column(cast_function_str: str, args: Optional[List[str]]):
    """Get the pyspark function to cast the column to the desired type.

    see https://spark.apache.org/docs/latest/sql-ref-datatypes.html for more info

    Args:
        cast_function_str (str): Function string to search in pyspark.sql.types
        args (Optional[List[str]]): Arguments of the functions

    Returns:
        TODO: See what is the return type
        _type_: Function to cast the column
    """
    cast_function = getattr(pyspark.sql.types, cast_function_str)
    if args:
        return cast_function(*args)
    else:
        return cast_function()


def _tbl_to_csv(file_name: str, input_folder: str, output_folder: str):
    """Tranform a catwise tbl to parquet and selects the given columns.

    Args:
        file_name (str): name of the file to transform
        input_folder (str): Folder where the files will be read
        output_folder (str): Folder where the files will be saved
    """
    log.info(f"Tranforming {file_name} into a csv file")
    try:
        catwise_table = _read_catwise_tbl_gz(input_folder.joinpath(file_name))

        columns_map_info = DOWNLOAD_CATALOG_DICT["catwise"]["columns_map"]

        # Casting DataTypes and selecting columns
        catwise_table_casted = catwise_table.select(
            *(
                col(c).cast(
                    _cast_column(
                        columns_map_info[c]["type"], columns_map_info[c]["args"]
                    )
                )
                for c in list(DOWNLOAD_CATALOG_DICT["catwise"]["columns_map"].keys())
            )
        )

        try:
            catwise_table_casted.write.option("header", False).csv(
                str(output_folder.joinpath(file_name.replace("tbl.gz", "csv")))
            )
        except:
            log.info(
                f"{str(output_folder.joinpath(file_name.replace('tbl.gz', 'csv')))} Already exist"
            )
        try:
            os.remove(input_folder.joinpath(file_name))
        except:
            pass

    except OSError or EOFError:
        log.info(
            f"Could not convert {file_name}, because is not a gzip file or it is corrupted"
        )


if __name__ == "__main__":
    spark = start_spark()

    log.info(
        f"Selecting the following columns {DOWNLOAD_CATALOG_DICT['catwise']['columns_map']}"
    )

    raw_catwise_path = RAW_DATA_PATH.joinpath("catwise")

    intermediate_catwise_path = INTERMEDIATE_DATA_PATH.joinpath("catwise")

    try:
        os.mkdir(intermediate_catwise_path)
    except:
        log.info(f"Directory {intermediate_catwise_path} already exists")
        pass

    files = os.listdir(raw_catwise_path)

    for file_name in files:
        if file_name.endswith(".tbl.gz"):
            _tbl_to_csv(
                file_name,
                input_folder=raw_catwise_path,
                output_folder=intermediate_catwise_path,
            )
