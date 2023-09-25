import sys
import requests
import argparse 
import json
import time
import os
import pandas as pd

sys.path.append("src")

from utils.config_loader import load_configuration

config = load_configuration()

parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
parser.add_argument(
    "--cellid",
    dest="cellid",
    help="CellID to experiment",
)

args, _ = parser.parse_known_args()


load = requests.put(f"http://127.0.0.1:8000/catwise_parquet")


points = pd.read_csv(config["parameters"]["query"] + "tests/20_random_coords.csv", engine="pyarrow")

points = points.to_dict(orient="records")

print(points[:5], points[:5])
print(load.status_code)

# Warm up query
body = {
    "points": points,
    "arcseconds": 1,
    "arcminutes": 0,
    "catalog": "string",
}

r = requests.post("http://127.0.0.1:8000/geo_parquet", json=body)

exp_path = f"parquet/parquet/"

print(exp_path)

os.makedirs(name=config["parameters"]["experiments"] + exp_path + "results/", exist_ok=True)

# Experiments
for arcsec in [1, 2, 4, 8, 16, 32, 60]:
    time_df = pd.DataFrame()
    for experiment in [1, 2, 3, 4, 5]:
        print(f"Doing experiment for: arcsec={arcsec} and number={experiment}")
        body = {
            "points": points,
            "arcseconds": arcsec,
            "arcminutes": 0,
            "catalog": "string",
        }
        start_time = time.time()

        r = requests.post("http://127.0.0.1:8000/geo_parquet", json=body)
        total_time = (time.time() - start_time)
        time_df = pd.concat([
            time_df,
            pd.DataFrame({"experiment_number": [experiment], "execution_time": [total_time]})
        ])
        
        content = json.loads(r.content)
        points_df = pd.DataFrame(content["result"])
        points_df["experiment"] = experiment
        points_df.to_csv(config["parameters"]["experiments"] + exp_path + f"results/points_{arcsec}_arcsecs_expid_{experiment}.csv", index=False)
    time_df.to_csv(config["parameters"]["experiments"] + exp_path + f"time_experiments_{arcsec}_arcsecs.csv", index=False)