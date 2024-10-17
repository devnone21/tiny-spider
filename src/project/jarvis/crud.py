# from typing import Iterable
from pymongo.errors import BulkWriteError
# from ..database import mongo_conn
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def insert_list_of_dict(self, collection: str, data: list):
    n_inserted = -1
    try:
        db_collection = self.db[collection]
        res = db_collection.insert_many(data, ordered=False)
        n_inserted = len(res.inserted_ids)
        LOGGER.debug(f'({collection}) nInserted: {n_inserted}')
    except BulkWriteError as err:
        n_errors = len(err.details.get('writeErrors'))
        n_inserted = int(err.details.get('nInserted'))
        LOGGER.debug(f'({collection}) nInserted: {n_inserted}, writeErrors: {n_errors}')
    except AttributeError as err:
        LOGGER.error(err)
    finally:
        return n_inserted
