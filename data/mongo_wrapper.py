from typing import Optional, List, Tuple

from pandas import DataFrame
from pymongo import MongoClient
from configuration import dbconfig
import certifi


class MongoDB:
    # General-purpose wrapper class for MongoDB Atlas document database

    def __init__(self, **kwargs):

        conn_string = "mongodb+srv://{}:{}@{}.mongodb.net/{}?retryWrites=true&w=majority".format(
            dbconfig.MONGODB_USER,
            dbconfig.MONGODB_PW,
            dbconfig.MONGODB_CLUSTER,
            dbconfig.MONGODB_DEFAULT_DB
        )
        self.client = MongoClient(conn_string, **kwargs, tlsCAFile=certifi.where())

    def find(self, db: str, collection: str, query: dict, projection: Optional[dict] = None,
             limit: Optional[int] = 10, orderby: Optional[Tuple or List] = None):

        if limit == 1 and orderby is None:
            return [self.client[db][collection].find_one(query, projection)]
        else:
            cur = self.client[db][collection].find(query, projection)

            if orderby is not None:
                if isinstance(orderby, tuple):
                    orderby = [orderby]
                cur.sort(orderby)

            if limit is not None:
                cur.limit(limit)

            out = list(cur)
            return out

    def insert(self, db: str, collection: str, x: DataFrame or list or dict):

        if isinstance(x, DataFrame):
            if x.index.name is not None:
                x.reset_index(inplace=True)

            x = x.to_dict(orient="records")

        if isinstance(x, dict):
            x = [x]

        n_toinsert = len(x)
        status = {"inserted": 0,
                  "updated": 0,
                  "not_inserted": n_toinsert}

        if n_toinsert == 1:
            result = self.client[db][collection].insert_one(x[0])
            status["inserted"] = 1 if result.inserted_id is not None else 0
        elif n_toinsert > 1:
            result = self.client[db][collection].insert_many(x)
            status["inserted"] = len(result.inserted_ids)

        status["not_inserted"] = n_toinsert - status["inserted"]
        return status

    def update(self, db: str, collection: str, query: dict, newvalues: dict, only_one: Optional[bool] = False):

        status = {"updated": 0}

        if only_one:
            result = self.client[db][collection].update_one(query, newvalues)
        else:
            result = self.client[db][collection].update_many(query, newvalues)

        status["updated"] = result.modified_count
        return status

    def delete(self, db: str, collection: str, query: dict,
               only_one: Optional[bool] = False, delete_all: Optional[bool] = False):

        status = {"deleted": 0}
        result = None

        if only_one:
            result = self.client[db][collection].delete_one(query)
        elif (delete_all == False and query != {}) or (delete_all == True and query == {}):
            result = self.client[db][collection].delete_many(query)

        status["deleted"] = result.deleted_count
        return status

    def count_documents(self, db: str, collection: str, query: dict):
        return self.client[db][collection].count_documents(query)

    def get_collection_size(self, db: str, collection: str):
        return self.count_documents(db, collection, {})

    def bulk_execute(self, db: str, collection: str, operations: List, ordered: Optional[bool] = True):
        result = self.client[db][collection].bulk_write(operations, ordered=ordered)
        return {"matched": result.matched_count,
                "inserted": result.inserted_count,
                "modified": result.modified_count,
                "upserted": result.upserted_count,
                "deleted": result.deleted_count}
