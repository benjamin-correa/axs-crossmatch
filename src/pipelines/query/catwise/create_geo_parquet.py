import logging
import os
import sys

from sedona.core.enums import IndexType
from sedona.utils.adapter import Adapter
from sedona.utils.adapter import SedonaPairRDD
from shapely.geometry import Point
from sedona.core.SpatialRDD import CircleRDD
from sedona.core.spatialOperator import JoinQueryRaw
import cProfile

from pyspark.sql.functions import col
sys.path.append("src")

from utils.global_constants import (
    INTERMEDIATE_DATA_PATH,
    PRIMARY_DATA_PATH,
    QUERY_DATA_PATH,
)  # noqa E402

from utils.global_utils import start_spark  # noqa E402


log = logging.getLogger(__name__)


if __name__ == "__main__":
    # with cProfile.Profile() as pr:
    spark = start_spark()
    sc = spark.sparkContext


    file_path = PRIMARY_DATA_PATH + "catwise/catwise/*.parquet"
    # file_path = PRIMARY_DATA_PATH + "catwise.csv"
    log.info("Reading folder: %s", file_path)

    catwise_df = spark.read.parquet(file_path)

    catwise_df = catwise_df.withColumn("ra_point", catwise_df.ra_point-180)
    catwise_df = catwise_df.withColumn("dec_point",col("dec_point").cast('double'))

    catwise_df.describe(["ra_point", "dec_point"]).show()

    # ra -> longitude
    # dec -> latitude
    # (longitude, latitude)

    catwise_df.printSchema()

    catwise_df.createOrReplaceTempView("table")

    catwise_sdf = spark.sql(
        f"""
        SELECT
            {', '.join(catwise_df.columns[2:])}, ST_Point(ra_point, dec_point) as geom, ST_GeoHash(ST_Point(ra_point, dec_point), 7) as geohash
        FROM
            table
        ORDER BY geohash
        """
    )

    # Debugging porpuses

    # import pyspark.sql.functions as F
    # catwise_sdf.groupBy("geohash").count().orderBy(col("count").desc()).show()

    # catwise_sdf = catwise_sdf.orderBy("geohash")

    # catwise_sdf.groupBy("geohash").count().orderBy(col("count").desc()).show()

    catwise_sdf.explain("formatted")
    catwise_sdf.write.format("geoparquet").save(QUERY_DATA_PATH + "catwise/catwise_geohash_7.parquet")
    log.info("Table with geo hashes created and saved succesfully")
    # pr.print_stats()