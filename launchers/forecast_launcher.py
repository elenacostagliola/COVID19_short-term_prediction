import sys, os
from pathlib import Path

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from pipelines import forecast

if __name__ == '__main__':

    STEPS = 7

    folder = os.listdir(Path(f"{ROOT_FOLDER}/pipelines/forecast_configuration"))
    for i in folder:
        if i.endswith(".json"):
            print("Launching Forecast pipeline with config file", i)
            forecast.main(Path(f"{ROOT_FOLDER}/pipelines/forecast_configuration/{i}"), steps=STEPS)
