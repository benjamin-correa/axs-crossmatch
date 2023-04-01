# BUILD COORDINATES
import logging
import re
from random import uniform

import pandas as pd
from shapely import wkt
from shapely.geometry import Point

log = logging.getLogger(__name__)
if __name__ == "__main__":
    points = []
    ra_list = []
    dec_list = []
    radius = uniform(0.05, 1)
    for i in range(50000):
        ra = uniform(0.0, 1.0)
        dec = uniform(-90.0, 90.0)
        log.info(f"Making query with ra={ra}, dec={dec}, radius={radius}")
        range_query_window = Point(ra, dec)
        points.append(range_query_window)
        ra_list.append(ra)
        dec_list.append(dec)
    df = pd.DataFrame({"geometry": points, "ra": ra_list, "dec": dec_list})
    df.to_csv("data/03_primary/catwise/test_points.csv", index=False)
    df.sample(1).to_csv("data/03_primary/catwise/test_one_point.csv", index=False)
    # gdf.to_csv("data/03_primary/catwise/test_points.csv", index=False)
    # gdf.to_file("data/03_primary/catwise/test_points.geojson", driver='GeoJSON')
