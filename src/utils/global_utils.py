import logging
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Union

import yaml
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer
from sedona.utils import SedonaKryoRegistrator

log = logging.getLogger(__name__)


def read_yaml(file_name: Union[str, Path], encoding: str = "utf8") -> Dict[str, Any]:
    """Read YAML files with configurations."""
    log.info(f"READING: {file_name}")
    with open(file_name, "r", encoding=encoding) as file:
        config_dict = yaml.safe_load(file)
    return config_dict


def start_spark():
    """Initialize Spark."""
    # noqa D202
    def create_session(
        master: Optional[str] = "local[*]", app_name: Optional[str] = "my_app"
    ) -> SparkSession:
        """Create a spark session."""
        extra_jars = load_extra_jars()

        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.serializer", KryoSerializer.getName)
            .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
            .config("spark.jars", ",".join(extra_jars))
            .getOrCreate()
        )

        return spark

    def load_extra_jars() -> list:
        """Load extra jars."""
        extra_jars_dir = os.path.join(os.environ["SPARK_HOME"], "extra_jars")
        return [
            os.path.join(extra_jars_dir, x)
            for x in os.listdir(os.path.join(os.environ["SPARK_HOME"], "extra_jars"))
        ]

    def set_logging(spark: SparkSession, log_level: Optional[str] = None) -> None:
        """Set log level - ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN."""
        if isinstance(spark, SparkSession):
            spark.sparkContext.setLogLevel(log_level) if isinstance(
                log_level, str
            ) else None

    spark = create_session(app_name="sedona")
    set_logging(spark, log_level="ERROR")
    SedonaRegistrator.registerAll(spark)

    return spark
