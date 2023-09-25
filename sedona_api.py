from fastapi import FastAPI
import logging
from src.utils.global_utils import start_spark
from pprint import pformat
from src.utils.config_loader import load_configuration
from pydantic import BaseModel
from pyspark.sql.functions import col
from pyspark import StorageLevel
from src.utils.global_utils import arcsec_to_degrees
import io
import pandas as pd

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y/%m/%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

app = FastAPI()

spark = start_spark()


config = load_configuration()
logger.info("Configuration used in Spark:\n%s", pformat(config))
catalog_columns = list(config["catwise"]["catwise"]["columns_map"].keys())
catalog_columns = ["CAST(" + column + " AS STRING)" for column in catalog_columns]


@app.get("/config")
async def get_config():
    return {"app_config": config, "spark_config": spark.sparkContext._conf.getAll()}


@app.put("/catwise_geo_parquet/{geo_hash}")
def initialize_catwise(geo_hash: int):
    file_path = config["parameters"]["query"] + "catwise/" + f"catwise_geohash_{geo_hash}_subset.parquet"
    catwise_df = spark.read.format("geoparquet").load(file_path)
    catwise_df = catwise_df.repartition(catwise_df.rdd.getNumPartitions() * 8)
    catwise_df.createOrReplaceTempView("catwise")
    return {"msg": "Catwise initialized", "schema": catwise_df.schema}

@app.put("/catwise_parquet")
def initialize_catwise():
    file_path = config["parameters"]["query"] + "catwise/" + f"catwise.parquet"
    catwise_df = spark.read.format("parquet").load(file_path)
    catwise_df = catwise_df.persist(StorageLevel.DISK_ONLY)
    catwise_df.createOrReplaceTempView("catwise")
    return {"msg": "Catwise initialized", "schema": catwise_df.schema}

@app.put("/catwise_equi_join")
def initialize_catwise():
    file_path = config["parameters"]["query"] + "catwise/" + f"catwise_equi_join_9_sample.parquet"
    catwise_df = spark.read.format("parquet").load(file_path)
    catwise_df = catwise_df.persist(StorageLevel.DISK_ONLY)
    catwise_df.createOrReplaceTempView("catwise")
    return {"msg": "Catwise initialized", "schema": catwise_df.schema}

class Query(BaseModel):
    points: list[dict[str, float]]
    arcseconds: float
    arcminutes: float
    catalog: str


@app.post('/geo_parquet')
def crossmatch(query: Query):

    query_dict = query.dict()

    points_df = spark.createDataFrame(query_dict["points"])

    degrees = arcsec_to_degrees(query_dict["arcminutes"], query_dict["arcseconds"])
    points_df.createOrReplaceTempView("points")
    points_df = spark.sql(
    f"""
    SELECT
        ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}) as geom,
        ST_Point(p.ra - 180, p.dec) as query_point
    FROM points p
    """
    )
    points_df.createOrReplaceTempView("points")

    intersection = spark.sql(
        f"""
        SELECT
            CAST(c.geom AS STRING) as point,
            CAST(p.query_point AS STRING) as query_point
        FROM
            catwise c, points p
        WHERE
            ST_Intersects(c.geom, p.geom)         
        """
    )
    return {"result": intersection.toPandas()}

class QueryEquiJoin(BaseModel):
    points: list[dict[str, float]]
    arcseconds: float
    arcminutes: float
    catalog: str
    s2_hash: int


@app.post('/equijoin_refined')
async def equijoin(query: QueryEquiJoin):

    query_dict = query.dict()

    points_df = spark.createDataFrame(query_dict["points"])
    degrees = arcsec_to_degrees(query_dict["arcminutes"], query_dict["arcseconds"])

    points_df.createOrReplaceTempView("points")
    points_df = spark.sql(
    f"""
    SELECT DISTINCT
        ST_Point(p.ra - 180, p.dec) as point,
        ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}) as geom,
        explode(ST_S2CellIDs(ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}), 9)) as cellId
    FROM points p
    """
    )

    points_df.createOrReplaceTempView("points")

    intersection = spark.sql(
        f"""
        SELECT
            CAST(c.geom AS STRING) as point,
            CAST(p.point AS STRING) as query_point,
            {', '.join(catalog_columns)}
        FROM
            catwise AS c, points AS p
        WHERE
            c.col = p.cellId AND ST_Intersects(p.geom, c.geom)
        """
    )
    return {"count": intersection.count(), "result": intersection.toPandas()}



@app.post('/hybrid_equijoin_refined')
async def equijoin(query: QueryEquiJoin):
    query_dict = query.dict()

    points_df = spark.createDataFrame(query_dict["points"])
    degrees = arcsec_to_degrees(query_dict["arcminutes"], query_dict["arcseconds"])

    points_df.createOrReplaceTempView("points")
    points_df = spark.sql(
    f"""
    SELECT DISTINCT
        ST_Point(p.ra - 180, p.dec) as point,
        ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}) as geom,
        explode(ST_S2CellIDs(ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}), 9)) as cellId,
        explode(ST_S2CellIDs(ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}), 4)) as partitionId
    FROM points p
    """
    )

    points_df.createOrReplaceTempView("points")

    intersection = spark.sql(
        f"""
        SELECT
            CAST(c.geom AS STRING) as point,
            CAST(p.point AS STRING) as query_point
        FROM
            catwise AS c, points AS p
        WHERE
            c.cellId = p.cellId
            AND c.partitionId = p.partitionId
            AND ST_Intersects(p.geom, c.geom)
        """
    )
    return {"result": intersection.toPandas()}




@app.post('/equi_join_query_count')
async def equijoin(query: QueryEquiJoin):
    query_dict = query.dict()

    points_df = spark.createDataFrame(query_dict["points"])
    degrees = arcsec_to_degrees(query_dict["arcminutes"], query_dict["arcseconds"])

    original_count = points_df.count()

    points_df.createOrReplaceTempView("points")
    points_df = spark.sql(
    f"""
    SELECT DISTINCT
        ST_Point(p.ra - 180, p.dec) as point,
        ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}) as geom,
        explode(ST_S2CellIDs(ST_Buffer(ST_Point(p.ra - 180, p.dec), {degrees}), {query_dict["s2_hash"]})) as cellId
    FROM points p
    """
    )

    new_count = points_df.count()

    return {"original_count": original_count, "new_count": new_count, "relative_diff": float(new_count)/float(original_count)}




@app.post('/equijoin_query_test')
async def equijoin(query: QueryEquiJoin):
    query_dict = query.dict()

    points_df = spark.createDataFrame(query_dict["points"])
    degrees = arcsec_to_degrees(query_dict["arcminutes"], query_dict["arcseconds"]) * 3
    import math


    # 1 / lat))
    points_df.createOrReplaceTempView("points")
    points_df = spark.sql(
    f"""
    SELECT DISTINCT
        p.ra as pointRa,
        p.dec as pointDec,
        ln(tan(radians({degrees})) + sec(radians({degrees}))) as degrees,
        {str(query_dict["arcseconds"])} as arcseconds,
        CAST(ST_Area(ST_Buffer(ST_Point(p.dec, p.ra - 180), {degrees})) AS STRING) AS area,
        CAST(ST_Point(p.dec, p.ra - 180) AS STRING) as point,
        CAST(ST_Buffer(ST_Point(p.dec, p.ra - 180), {degrees}) AS STRING) as geom
    FROM points p
    """
    )

    df = points_df.toPandas()


    return {"result": df}
