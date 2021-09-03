import argparse
import sys, os
from pandas import Timestamp

ROOT_FOLDER = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)))

sys.path.insert(0, ROOT_FOLDER)

from updaters import *


def main(selected_updaters=None, skip=None, all=False, debug=False):
    execution_timestamp = Timestamp.utcnow()
    success = {}

    # Pick chosen updaters (each option overrides the previous)
    updaters = None
    if selected_updaters is not None:
        updaters = {}
        for u in selected_updaters:
            if u in avail_updaters:
                updaters[u] = avail_updaters[u]

    if skip is not None:
        updaters = avail_updaters
        for u in skip:
            if u in avail_updaters:
                del updaters[u]

    if all:
        updaters = avail_updaters

    assert updaters is not None

    # Open log file
    f = open(ROOT_FOLDER + f"/logs/data_update_{execution_timestamp.strftime('%Y%m%d%H%M%S')}.txt", "a")
    f.write(f"Pipeline started at: {execution_timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\nReport:\n")
    f.flush()

    ti = Timestamp("now")
    # Orchestrates all updaters

    for u in updaters:
        tci = Timestamp("now")
        print("Running updater for entity " + u, end="")
        status = "failed"
        n_records = 0
        if debug:
            n_records = updaters[u].run()
            status = "success"
        else:
            try:
                n_records += updaters[u].run()
                status = "success"
            except:
                status = "failed"
        tcf = Timestamp("now")
        if status == "success":
            print(f" --> {status} | records: {n_records} | elapsed time: {tcf - tci}")
        elif status == "failed":
            print(" --> failed")

        # Write log
        f.write(f"{u} --> status: {status} | records: {n_records} | elapsed time: {tcf - tci}\n")
        f.flush()

    tf = Timestamp("now")
    print("Done.")
    print("Total elapsed time: " + str(tf - ti))

    # Write log
    f.write(f"\nTotal execution time: {tf - ti}")
    f.flush()
    f.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sequential launcher for the updaters.')
    parser.add_argument('--updaters', type=str, nargs='+',
                        help='run specific updaters: pass their names')
    parser.add_argument('--skip', type=str, nargs='+',
                        help='run all updaters except these')
    parser.add_argument('--all', action="store_true",
                        help='run all updaters')
    parser.add_argument('--debug', action="store_true",
                        help='run in debug mode (throws exceptions)')
    args = parser.parse_args()

    main(selected_updaters=args.updaters, skip=args.skip, all=args.all, debug=args.debug)
