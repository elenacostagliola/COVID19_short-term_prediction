import sys, os

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from pipelines import data_curation
from launchers import data_update_launcher


def main():
    data_update_launcher.main()
    print("Running data curation pipeline")
    data_curation.main()


if __name__ == '__main__':
    main()
