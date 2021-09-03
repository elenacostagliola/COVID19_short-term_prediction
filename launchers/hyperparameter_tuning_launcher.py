import sys, os
from pathlib import Path

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from pipelines import hyperparameter_tuning

if __name__ == '__main__':
    folder = os.listdir(Path(f"{ROOT_FOLDER}/pipelines/hyperparameter_tuning_configurations"))
    for i in folder:
        if i.endswith(".json"):
            print("Launching Hyperparameter Tuning pipeline with config file", i)
            hyperparameter_tuning.main(Path(f"{ROOT_FOLDER}/pipelines/hyperparameter_tuning_configurations/{i}"))
