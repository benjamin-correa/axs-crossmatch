import logging
import os
import sys
from multiprocessing import Process
from typing import List

import numpy as np
import requests
import wget
from bs4 import BeautifulSoup

sys.path.append("src")
from utils.global_constants import DOWNLOAD_CATALOG_DICT, RAW_DATA_PATH  # noqa E402


log = logging.getLogger(__name__)


def _download_wget(folder_url: str, elements: List[str]):
    """Use wget to download the data from the catalog.

    Args:
        folder_url (str): Url that will be used to download the file
        el (str): element to download
    """
    for element in elements:
        if "rej" in element:
            continue
        elif os.path.exists(RAW_DATA_PATH.joinpath("catwise/", element)):
            log.info(
                f"File already exists in: {RAW_DATA_PATH.joinpath('catwise/', element)}"
            )
        else:
            log.info(
                f"Downloading: {folder_url}{element} -> {RAW_DATA_PATH.joinpath('catwise/', element)}"
            )
            wget.download(
                folder_url + element,
                f"{RAW_DATA_PATH.joinpath('catwise/', element)}",
                bar=None,
            )


def _download_folder(folder_num: int, catalog_url: str):
    """Download the CatWISE folders.

    Args:
        folder_num (int): Number of the folder to download
        catalog (str): Name of the catalog to download
    """
    folder_url = catalog_url + str(folder_num).zfill(3) + "/"

    log.info("Downloading from " + folder_url)
    response = requests.get(folder_url)
    soup = BeautifulSoup(response.text, "html.parser")
    links = [a.get("href") for a in soup.find_all("a", href=True)][5:]
    try:
        os.mkdir(RAW_DATA_PATH.joinpath("catwise"))
    except FileExistsError:  # Directory already exists
        pass

    # initialisation
    processes = []
    number_processes = 4
    files = np.array_split(links, number_processes)
    # creation of the processes and their result queue
    for i in range(number_processes):
        try:
            processes.append(
                Process(
                    target=_download_wget,
                    args=(
                        folder_url,
                        files[i],
                    ),
                )
            )
            processes[i].start()
        except IndexError:  # Index out of range, the links remaining are less than the number of processes
            break

    # Wait for a process to terminate and get its result from the queue
    for i in range(number_processes):
        try:
            processes[i].join()
        except IndexError:  # Index out of range, the links remaining are less than the number of processes
            break
    processes = []


if __name__ == "__main__":
    catalog_data = DOWNLOAD_CATALOG_DICT["catwise"]
    for folder_number in range(
        catalog_data["start_folder"], catalog_data["end_folder"]
    ):
        _download_folder(folder_number, catalog_data["url"])
