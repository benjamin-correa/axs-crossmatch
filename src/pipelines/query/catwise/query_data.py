import logging
import os
import sys

from sedona.core.enums import IndexType
from sedona.core.spatialOperator import RangeQuery
from sedona.utils.adapter import Adapter
from sedona.utils.adapter import SedonaPairRDD
from shapely.geometry import Point

sys.path.append("src")

from utils.global_constants import (
    INTERMEDIATE_DATA_PATH,
    PRIMARY_DATA_PATH,
)  # noqa E402

from utils.global_utils import start_spark  # noqa E402


log = logging.getLogger(__name__)


def _read_parquet(file_path: str):
    return spark.read.parquet(file_path)


from sedona.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD

if __name__ == "__main__":

    spark = start_spark()
    sc = spark.sparkContext
    primary_catwise_path = PRIMARY_DATA_PATH.joinpath("catwise")

    file_path = str(primary_catwise_path.joinpath("catwise.csv"))

    catwise_df = spark.read.format("csv").load(file_path)
    # catwise_df = _read_parquet(file_path)
    # catwise_df.createOrReplaceTempView("catwise")

    # counties_geom = spark.sql(
    #     """
    #     SELECT
    #         *
    #     FROM
    #         catwise
    #     LIMIT 10
    #     """
    # )
    # print(counties_geom.first())
    # print(hola)

    from pyspark import StorageLevel
    from sedona.core.enums import FileDataSplitter

    offset = 0  # The point long/lat starts from Column 0
    splitter = FileDataSplitter.WKT  # FileDataSplitter enumeration
    carry_other_attributes = True  # Carry Column 2 (hotel, gas, bar...)
    level = StorageLevel.MEMORY_ONLY  # Storage level from pyspark
    query_window_rdd = PointRDD(
        sparkContext=sc,
        InputLocation="data/03_primary/catwise/test_points.csv",
        Offset=offset,
        splitter=splitter,
        carryInputData=carry_other_attributes,
        newLevel=level,
    )

    offset = 0  # The point long/lat starts from Column 0
    splitter = FileDataSplitter.WKT  # FileDataSplitter enumeration
    carry_other_attributes = True  # Carry Column 2 (hotel, gas, bar...)
    level = StorageLevel.MEMORY_ONLY  # Storage level from pyspark
    s_epsg = "epsg:4326"  # Source epsg code
    t_epsg = "epsg:3857"  # target epsg code
    spatial_rdd = PointRDD(
        sparkContext=sc,
        InputLocation=file_path,
        Offset=offset,
        splitter=splitter,
        carryInputData=carry_other_attributes,
        newLevel=level,
    )
    # print(query_window_rdd.rawSpatialRDD.take(1))

    consider_boundary_intersection = (
        False  ## Only return gemeotries fully covered by the window
    )

    build_on_spatial_partitioned_rdd = True  ## Set to TRUE only if run join query

    circle_rdd = CircleRDD(query_window_rdd, 0.006)

    log.info("Building the tree")
    from sedona.core.enums import GridType

    spatial_rdd.spatialPartitioning(GridType.KDBTREE)
    circle_rdd.spatialPartitioning(spatial_rdd.getPartitioner())
    circle_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)
    log.info("Making the query")
    using_index = True

    from sedona.core.spatialOperator import JoinQueryRaw

    result_test: SedonaPairRDD = JoinQueryRaw.DistanceJoinQueryFlat(
        spatial_rdd, circle_rdd, using_index, consider_boundary_intersection
    )
    result_test_rdd = result_test.to_rdd()
    print(result_test_rdd.map(lambda x: [x[0].geom, x[1].geom]).collect())

    import time

    # get the start time
    st = time.process_time()
    st_sec = time.time()

    result1: SedonaPairRDD = JoinQueryRaw.DistanceJoinQuery(
        spatial_rdd, circle_rdd, using_index, consider_boundary_intersection
    )
    results_rdd_1 = result1.to_rdd()
    print(
        results_rdd_1.map(
            lambda x: [x[0].geom, [result.geom for result in x[1]]]
        ).collect()
    )

    # get the end time
    et = time.process_time()

    elapsed_time = time.time() - st_sec
    print(f'Execution time: {time.strftime("%H:%M:%S", time.gmtime(elapsed_time))}')
    # get execution time
    res = et - st
    log.info(f"CPU Execution time: {res} seconds")
