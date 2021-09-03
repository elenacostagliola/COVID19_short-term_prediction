import os
from . import load_config

MYSQL_HOST = os.environ.get("MYSQL_HOST")
MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PW = os.environ.get("MYSQL_PW")
MYSQL_SCHEMA = os.environ.get("MYSQL_SCHEMA")

MONGODB_USER = os.environ.get("MONGODB_USER")
MONGODB_PW = os.environ.get("MONGODB_PW")
MONGODB_CLUSTER = os.environ.get("MONGODB_CLUSTER")
MONGODB_DEFAULT_DB = os.environ.get("MONGODB_DEFAULT_DB")

__missing_ev_count = 0
for ev in [MYSQL_HOST, MYSQL_USER, MYSQL_PW, MYSQL_SCHEMA, MONGODB_USER, MONGODB_PW, MONGODB_CLUSTER,
           MONGODB_DEFAULT_DB]:
    if ev is None:
        __missing_ev_count += 1

if __missing_ev_count > 0:
    print(f"[Error] Missing {__missing_ev_count} DB credentials environment variables;"
          f"make sure to set all required environment variables.")
    exit(-1)
