from pymongo.errors import BulkWriteError

from ..share.database import mongo_conn

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def find_all(self, collection: str):
    try:
        db_collection = self.db[collection]
        with db_collection.find() as cursor:
            res = [doc for doc in cursor]
            if res:
                logger.debug(f'({collection}) found')
        return res
    except TypeError as err:
        logger.error(err)


def upsert_one(self, collection: str, match: dict, data: dict):
    n_upsert = -1
    try:
        db_collection = self.db[collection]
        res = db_collection.update_one(
            filter=match, update={'$set': data},
            upsert=True
        )
        n_upsert = res.modified_count
        logger.debug(f'({collection}) upsert: {match}')
    except AttributeError as err:
        logger.error(err)
    finally:
        return n_upsert


def insert_list_of_dict(self, collection: str, data: list):
    n_inserted = -1
    try:
        db_collection = self.db[collection]
        res = db_collection.insert_many(data, ordered=False)
        n_inserted = len(res.inserted_ids)
        logger.debug(f'({collection}) nInserted: {n_inserted}')
    except BulkWriteError as err:
        n_errors = len(err.details.get('writeErrors'))
        n_inserted = int(err.details.get('nInserted'))
        logger.debug(f'({collection}) nInserted: {n_inserted}, writeErrors: {n_errors}')
    except AttributeError as err:
        logger.error(err)
    finally:
        return n_inserted
