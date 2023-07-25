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

sys.path.append("src")

from utils.global_constants import (
    INTERMEDIATE_DATA_PATH,
    PRIMARY_DATA_PATH,
    QUERY_DATA_PATH,
)  # noqa E402

from utils.global_utils import start_spark  # noqa E402


log = logging.getLogger(__name__)


if __name__ == "__main__":
    with cProfile.Profile() as pr:
        spark = start_spark()
        sc = spark.sparkContext

        primary_catwise_path = PRIMARY_DATA_PATH + "catwise/"

        # file_path = primary_catwise_path + "catwise/*.parquet"
        file_path = primary_catwise_path + "catwise.csv"
        log.info("Reading folder: %s", file_path)

        catwise_df = spark.read.csv(file_path)

        catwise_df = catwise_df.withColumn("_c0", catwise_df._c0-180)

        catwise_df.createOrReplaceTempView("table")

        catwise_sdf = spark.sql(
            f"""
            SELECT
                ST_Point(_c1, _c0) AS geom, {', '.join(catwise_df.columns[2:])}
            FROM
                table
            WHERE
                _c1 <= 85.06 AND
                _c1 >= -85.06 
            """
        )
        spatial_rdd = Adapter.toSpatialRdd(catwise_sdf, "geom")
        spatial_rdd.analyze()
        sourceCrsCode = "epsg:4326" # WGS84, the most common degree-based CRS
        targetCrsCode = "epsg:3857" # The most common meter-based CRS
        spatial_rdd.CRSTransform(sourceCrsCode, targetCrsCode)


        points_df = spark.read.csv("data/04_query/tests/randomequator_1000coords_5deg.txt")

        points_df = points_df.withColumn("_c0", points_df._c0-180)

        points_df.createOrReplaceTempView("points")

        points_sdf = spark.sql(
            f"""
            SELECT
                ST_Point(_c1, _c0) AS geom, _c0, _c1
            FROM
                points
            """
        )
        circle_rdd = Adapter.toSpatialRdd(points_sdf, "geom")
        sourceCrsCode = "epsg:4326" # WGS84, the most common degree-based CRS
        targetCrsCode = "epsg:3857" # The most common meter-based CRS
        circle_rdd.CRSTransform(sourceCrsCode, targetCrsCode)
        circle_rdd.analyze()
        circle_rdd = CircleRDD(circle_rdd, 0.1) ## Create a CircleRDD using the given distance
        circle_rdd.analyze()

        from sedona.core.enums import GridType

        build_on_spatial_partitioned_rdd = True
        spatial_rdd.spatialPartitioning(GridType.KDBTREE)
        circle_rdd.spatialPartitioning(spatial_rdd.getPartitioner())
        # spatial_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

        using_index = False
        consider_boundary_intersection = (
            False  # Only return gemeotries fully covered by the window
        )

        result1= JoinQueryRaw.DistanceJoinQuery(
            spatial_rdd, circle_rdd, using_index, consider_boundary_intersection
        )
        
        results_rdd_1 = result1.to_rdd()
        results_rdd_1 = results_rdd_1.map(
                lambda x: [
                    str(x[0].geom), str(x[0].userData),
                    str([[str(result.geom), str(result.userData)] for result in x[1]]),
                ]
            )
        from pyspark.sql.functions import col
        results_df = results_rdd_1.toDF(("point_1_geom", "point_2_geom", "results"))
        results_df.printSchema()
        # results_df = results_df.select([col(c).cast("string") for c in results_df.columns])
        results_df.write.mode("overwrite").csv(
            str("data/04_query/catwise/catwise_results.csv")
        )
    pr.print_stats()