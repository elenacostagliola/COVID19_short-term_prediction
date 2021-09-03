import sys, os

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from pipelines import data_update


def main():
    entities = ["ProvinceData", "Holiday", "RegionRisk", "VaccinesAdministrationData", "VaccinesDeliveryData",
                "StringencyIndex"]

    print("Launching Data Update pipeline")
    data_update.main(selected_updaters=entities)


if __name__ == '__main__':
    main()
