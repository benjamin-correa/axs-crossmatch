import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Union

import pandas as pd
import yaml

log = logging.getLogger(__name__)


def read_yaml(file_name: Union[str, Path], encoding: str = "utf8") -> Dict[str, Any]:
    """Read YAML files with configurations."""
    log.info(f"READING: {file_name}")
    with open(file_name, "r", encoding=encoding) as file:
        config_dict = yaml.safe_load(file)
    return config_dict


def read_catwise_tbl_gz(file_path: str) -> pd.DataFrame:
    """Read and parse a catwise .tbl.gz.

    Args:
        file_path (str): Path of the file

    Returns:
        pd.DataFrame: Parsed .tbl.gz
    """
    raw_table = pd.read_table(file_path, comment="\\", compression="gzip")

    # Getting the column names
    table_columns = raw_table.columns[0].lower().replace(" ", "").split("|")

    # Filtering empty list values
    table_columns = table_columns[1 : len(table_columns) - 1]

    # Removing first three rows
    cleaned_table = raw_table.iloc[3:]

    # Splitting the rows by " "
    table_data = cleaned_table.iloc[:, 0].str.split().to_list()

    # Creating final table
    final_table = pd.DataFrame(columns=table_columns, data=table_data)

    return final_table
