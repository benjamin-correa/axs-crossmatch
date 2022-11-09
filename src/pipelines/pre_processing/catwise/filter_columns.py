import logging
import os
import sys

sys.path.append("src")

from utils.global_utils import read_catwise_tbl_gz  # noqa E402
from utils.global_constants import (
    DOWNLOAD_CATALOG_DICT,
    RAW_DATA_PATH,
    INTERMEDIATE_DATA_PATH,
)  # noqa E402

log = logging.getLogger(__name__)


def tbl_to_parquet(file_name: str, input_folder: str, output_folder: str):
    """Tranform a catwise tbl to parquet and selects the given columns.

    Args:
        file_name (str): name of the file to transform
        input_folder (str): Folder where the files will be read
        output_folder (str): Folder where the files will be saved
    """
    log.info(f"Tranforming {file_name} into a parquet file")
    try:
        catwise_table = read_catwise_tbl_gz(input_folder.joinpath(file_name))

        catwise_table_filtered = catwise_table.loc[
            :, DOWNLOAD_CATALOG_DICT["catwise"]["columns"]
        ]

        catwise_table_filtered.to_parquet(
            output_folder.joinpath(file_name.replace("tbl.gz", "parquet")), index=False
        )
    except OSError or EOFError:
        log.info(
            f"Could not convert {file_name}, because is not a gzip file or it is corrupted"
        )


if __name__ == "__main__":
    log.info(
        f"Selecting the following columns {DOWNLOAD_CATALOG_DICT['catwise']['columns']}"
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
        tbl_to_parquet(
            file_name,
            input_folder=raw_catwise_path,
            output_folder=intermediate_catwise_path,
        )
