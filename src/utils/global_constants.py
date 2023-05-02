import logging
import os
import sys
from pathlib import Path

sys.path.append("src")

from utils.global_utils import read_yaml  # noqa E402


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y/%m/%d %H:%M:%S",
)

NUMBER_PROCESSES = 4

# ROOT_DIR = Path(os.getcwd())

CONGIF_FOLDER = "conf/"

GLOBALS_CONFIG = CONGIF_FOLDER + "globals.yml"

DOWNLOAD_CONFIG = CONGIF_FOLDER + "pre-processing/" + "pre-processing.yml"

DOWNLOAD_CATALOG_DICT = read_yaml(DOWNLOAD_CONFIG)

GLOBALS_CONFIG_DICT = read_yaml(GLOBALS_CONFIG)

RAW_DATA_PATH = GLOBALS_CONFIG_DICT["raw"]

INTERMEDIATE_DATA_PATH = GLOBALS_CONFIG_DICT["intermediate"]

PRIMARY_DATA_PATH = GLOBALS_CONFIG_DICT["primary"]
