import logging
import os
import sys

from sedona.core.enums import IndexType
from sedona.core.spatialOperator import RangeQuery
from sedona.utils.adapter import Adapter
from sedona.utils.adapter import SedonaPairRDD
from shapely.geometry import Point
from sedona.core.spatialOperator import JoinQueryRaw

from pyspark.sql.functions import col

sys.path.append("src")

from utils.global_constants import (
    INTERMEDIATE_DATA_PATH,
    PRIMARY_DATA_PATH,
    QUERY_DATA_PATH,
)  # noqa E402

from utils.global_utils import start_spark  # noqa E402


log = logging.getLogger(__name__)


from sedona.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD

if __name__ == "__main__":

    spark = start_spark()
    sc = spark.sparkContext

    query_catwise_path = QUERY_DATA_PATH + "catwise/"

    # file_path = query_catwise_path + "catwise.parquet"
    # catwise_df = spark.read.parquet(file_path)
    file_path = query_catwise_path + "catwise_geohash_7.parquet"
    catwise_df = spark.read.format("geoparquet").load(file_path)
    catwise_df.printSchema()
    catwise_df = catwise_df.withColumn("geom",col("geom").cast('string'))
    catwise_df.createOrReplaceTempView("catwise")
    print("hola")
    # catwise_df.select("geom").show(20, False)
    catwise_df.printSchema()
    print("chao")

    points_df = spark.read.csv("data/04_query/tests/randomequator_1000coords_5deg.txt")
    points_df.printSchema()


    points_df = points_df.withColumn("_c0", points_df._c0-180)
    points_df = points_df.withColumn("_c1",col("_c1").cast('double'))

    points_df.createOrReplaceTempView("points")

    # query = spark.sql(
    #     f"""
    #     SELECT
    #         c.geom as point
    #     FROM
    #         points p, catwise c
    #     WHERE
    #         ST_Contains(ST_Buffer(ST_Point(p._c0, p._c1), 0.001), ST_GeomFromWKT(c.geom)) == True
    #     """
    # )
    query = spark.sql(
        f"""
        SELECT
            c.geom as point
        FROM
            points p, catwise c
        WHERE
            ST_Intersects(ST_Buffer(ST_Point(p._c0, p._c1), 0.001), ST_GeomFromWKT(c.geom)) == True
        """
    )
    # query = query.filter(query["inter"] == True)
    query.explain("formatted")
    # query.show()

    # 1 arcsec tp deg 0.000277778

    query.write.mode("overwrite").csv(
        str("data/04_query/catwise/catwise_results.csv")
    )