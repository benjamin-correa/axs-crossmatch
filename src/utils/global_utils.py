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
