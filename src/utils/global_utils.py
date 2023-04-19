import logging
import os
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy as np
import pandas as pd
import yaml
from astropy import units as u
from astropy.coordinates import EarthLocation
from astropy.coordinates import SkyCoord
from astropy.coordinates import WGS84GeodeticRepresentation
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


def wgs84_to_string(wgs84_str: str) -> List[np.float64]:
    """Transform WGS84GeodeticRepresentation string to a tuple.

    Creates a tuple that consist in (lon, lat)

    Args:
        wgs84_str (str): String corresponding to the WGS84 coordinate

    Returns:
        tuple[float, float]: Tuple with (lon, lat) values
    """
    formatted_string = (
        wgs84_str.replace(", ", ",")
        .replace("(", "")
        .replace(")", "")
        .split()[0]
        .split(",")
    )
    return [np.float64(formatted_string[0]), np.float64(formatted_string[1])]


def transform_coordinates(
    catalog_df: pd.DataFrame, ra_col: str, dec_col: str, frame: str
) -> pd.DataFrame:
    """Transform coordinates.

    Uses SkyCoord to transform coordinates from any available frame to WGS84,
    see https://docs.astropy.org/en/stable/api/astropy.coordinates.SkyCoord.html for more info

    Args:
        catalog_df (pd.DataFrame): Dataframe which contains the catalog info
        ra_col (str): Column which contains ra in degrees
        dec_col (str): Column which contains dec in degrees
        frame (str): Frame of the coords

    Returns:
        pd.DataFrame: Dataframe with lon_wgs84 and lat_wgs84
    """
    log.info("transformando coordenadas")

    catalog_df["coords"] = SkyCoord(
        catalog_df[ra_col] * u.degree, catalog_df[dec_col] * u.degree, frame=frame
    )
    keck: EarthLocation = EarthLocation.of_site("Keck Observatory")
    keck_geo = keck.to_geodetic()
    print(keck_geo)
    print(
        EarthLocation.from_geodetic(
            catalog_df["coords"].iloc[0].ra, catalog_df["coords"].iloc[0].dec
        )
    )
    print(catalog_df["coords"].iloc[0])
    print(catalog_df["coords"].iloc[0].ra, catalog_df["coords"].iloc[0].dec)
    catalog_df["coords_wgs84"] = catalog_df["coords"].apply(
        lambda coord: WGS84GeodeticRepresentation(lon=coord.ra, lat=coord.dec)
    )
    catalog_df["coords_wgs84"] = catalog_df["coords_wgs84"].apply(
        lambda coord_str: wgs84_to_string(str(coord_str))
    )
    # log.info(catalog_df["coords_wgs84"].iloc[0].deg)
    split = pd.DataFrame(
        catalog_df["coords_wgs84"].to_list(), columns=["lon_wgs84", "lat_wgs84"]
    )

    catalog_df = pd.concat([catalog_df, split], axis=1)
    catalog_df.drop(columns=["coords", "coords_wgs84"])
    log.info(f"\n{catalog_df.loc[:, ['ra', 'dec', 'lon_wgs84', 'lat_wgs84']]}")
    return catalog_df


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
            .config("spark.executor.memory", "200G")
            .config("spark.driver.memory", "200G")
            .config("spark.driver.maxResultSize", "200G")
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

    spark: SparkSession = create_session(app_name="sedona")
    set_logging(spark, log_level="WARN")
    SedonaRegistrator.registerAll(spark)
    print(spark.sparkContext._conf.getAll())

    return spark
