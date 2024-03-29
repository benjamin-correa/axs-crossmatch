import logging
from typing import Optional, List
import sys

import pyspark.sql.types

from pyspark.sql.functions import col
sys.path.append("src")

from utils.global_constants import (
    PRIMARY_DATA_PATH,
    QUERY_DATA_PATH,
)  # noqa E402

from utils.config_loader import load_configuration

from utils.global_utils import start_spark  # noqa E402


log = logging.getLogger(__name__)

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

if __name__ == "__main__":
    # with cProfile.Profile() as pr:
    spark = start_spark()
    sc = spark.sparkContext

    config = load_configuration()
    file_path = PRIMARY_DATA_PATH + "catwise/catwise/*.parquet"
    # file_path = PRIMARY_DATA_PATH + "catwise.csv"
    log.info("Reading folder: %s", file_path)
    columns_map_info = {'ra_point': {'type': 'DoubleType', 'args': None}, 'dec_point': {'type': 'DoubleType', 'args': None}}
    columns_map_info.update(config["catwise"]["catwise"]["columns_map"])
    print(columns_map_info)

    column_names = list(columns_map_info.keys())
    raw_df = spark.read.parquet(file_path)
    catwise_df = raw_df.toDF(*column_names)

    # Casting DataTypes and selecting columns
    catwise_df = catwise_df.select(
        *(
            col(c).cast(
                _cast_column(
                    columns_map_info[c]["type"], columns_map_info[c]["args"]
                )
            )
            for c in list(columns_map_info.keys())
        )
    )

    catwise_df = catwise_df.withColumn("ra_point", catwise_df.ra_point-180)

    # catwise_df.describe(["ra_point", "dec_point"]).show()

    # ra -> longitude
    # dec -> latitude
    # (longitude, latitude)

    # catwise_df.printSchema()
    catwise_df.createOrReplaceTempView("table_limit")
    catwise_df = catwise_df.sample(fraction=0.5)

    catwise_df.createOrReplaceTempView("table")
    catwise_sdf = spark.sql(
        f"""
        SELECT
            {', '.join(catwise_df.columns[2:])}, ST_Point(ra_point, dec_point) as geom, ST_GeoHash(ST_Point(ra_point, dec_point), 9) as geohash
        FROM
            table
        ORDER BY geohash
        """
    )

    # Debugging porpuses
    # catwise_sdf.groupBy("geohash").count().orderBy(col("count").desc()).show()
    import pyspark.sql.functions as F
    catwise_sdf = catwise_sdf.withColumn('salt', F.rand())
    catwise_sdf = catwise_sdf.repartitionByRange(catwise_sdf.rdd.getNumPartitions() * 8, 'salt')

    catwise_sdf.explain("formatted")
    catwise_sdf.write.format("geoparquet").mode("overwrite").save(QUERY_DATA_PATH + "catwise/catwise_geohash_9_subset.parquet")
    log.info("Table with geo hashes created and saved succesfully")
    # pr.print_stats()