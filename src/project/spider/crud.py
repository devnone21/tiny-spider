from datetime import datetime, date, time, timedelta, timezone
from typing import List, Iterable, Any

from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from pymongo import ReplaceOne
from pymongo.database import Database
from pymongo.errors import BulkWriteError

from ..database import db_session, db_conn
from .models import Candle, CandleStat
from .schemas import CandleIn, CandleOut, CandleStatBase
from .XTBApi import Client, CommandFailed, SocketError
import logging
LOGGER = logging.getLogger("Spider.CRUD")
LOGGER.setLevel(logging.INFO)


def error_message(message):
    """Return: dict of error message."""
    return {"error": message}


# #
# Postgres
# #
def get_candles(symbol_id: int, timeframe_id: int, skip: int = 0, limit: int = 100):
    """Query candles from DB. Return: List of Candle object."""
    with db_session() as db:
        candles = db.query(Candle).filter(
            Candle.symbol_id == symbol_id,
            Candle.timeframe_id == timeframe_id
        ).offset(skip).limit(limit).all()
    if not candles:
        return None
    return [CandleOut(**candle.__dict__) for candle in candles]


def upsert_preserve(table: str, data: List[tuple], page_size: int = 1000) -> int:
    """PsycoPG2 batch upsert (on conflict id, do nothing). Return: number of inserted rows."""
    with db_conn() as conn:
        with conn.cursor() as cursor:
            execute_values(
                cursor,
                f"""
                INSERT INTO {table} VALUES %s ON CONFLICT (id) DO NOTHING; 
                """,
                data, page_size=page_size)
        conn.commit()
    return cursor.rowcount


def upsert_many_candles(candles: List[CandleIn]) -> int:
    """Upsert batch of candles. Return: number of inserted rows."""
    data = [candle.as_tuple() for candle in candles]
    return upsert_preserve(table='candles', data=data)


def query_ct(symbol_id: int, timeframe_id: int):
    """Query candles stat by symbol & period. Return: CandleStat object."""
    with db_session() as db:
        ct = db.query(CandleStat).filter(
            CandleStat.symbol_id == symbol_id,
            CandleStat.timeframe_id == timeframe_id
        ).first()
    if not ct:
        return None
    return CandleStatBase(**ct.__dict__)


def insert_ct(ct: CandleStatBase):
    """Insert candles stat. Return: CandleStat object."""
    db_ct = CandleStat(**ct.dict())
    with db_session() as db:
        db.add(db_ct)
        db.commit()
        db.refresh(db_ct)
    return ct


def update_ct(ct: CandleStatBase):
    """Update candles stat. Return: CandleStat object."""
    db_ct = CandleStat(**ct.dict())
    with db_session() as db:
        db.query(CandleStat).filter(
            CandleStat.symbol_id == ct.symbol_id,
            CandleStat.timeframe_id == ct.timeframe_id
        ).update({
            "date_from": db_ct.date_from,
            "date_until": db_ct.date_until,
            "digits": db_ct.digits,
        })
        db.commit()
    return ct


def upsert_ct(ct: CandleStatBase):
    """Upsert candles stat. Return: CandleStat object."""
    try:
        update_ct(ct)
    except OperationalError:
        insert_ct(ct)


# #
# Exchange API
# #
def _get_chart_from_ts(
        client: Client,
        ts:int,
        symbol: str,
        period: int,
        tick: int
) -> tuple[list, int]:
    """getChartRangeRequest function with retry"""
    default_result = ([], 0)
    try:
        res: dict = client.get_chart_range_request(symbol, period, ts, ts, tick)
        if not res.get('status', False):
            return default_result
    except (AttributeError, CommandFailed, SocketError) as err:
        print(err)
        res: dict = client.login()
        if not res.get('status', False):
            return default_result
        res: dict = client.get_chart_range_request(symbol, period, ts, ts, tick)
        if not res.get('status', False):
            return default_result

    return_data = res.get('returnData', {})
    digits: int = return_data.get('digits', 0)
    candles: list = return_data.get('rateInfos', [])
    return candles, digits


def gather_present_candles(ct: CandleStatBase, client: Client) -> tuple[list, int]:
    """get present charts"""
    ts = int(datetime.now(timezone.utc).timestamp())
    return _get_chart_from_ts(client, ts, ct.symbol, ct.period, tick=-300)


def _ct_max_backdate(timeframe) -> date:
    """Return suitable date to look back"""
    today_utc = datetime.now(timezone.utc).date()
    m = today_utc - timedelta(days=12*timeframe)
    if timeframe == 30:
        return max(m, date(2023, 7, 21))
    return m


def gather_olden_candles(ct: CandleStatBase, client: Client) -> tuple[list, int]:
    """get olden charts"""
    default_result = ([], 0)
    if _ct_max_backdate(ct.period) >= ct.date_from:
        return default_result
    ts = int(datetime.combine(ct.date_from, time(0, 0)).timestamp())
    return _get_chart_from_ts(client, ts, ct.symbol, ct.period, tick=-500)


# #
# MongoDB
# #
def insert_many(
        db: Database,
        collection: str,
        data: Iterable[dict[str, Any]]
) -> int:
    """MongoDB batch insert (on conflict _id, do nothing). Return: number of inserted rows."""
    n_inserted = -1
    try:
        res = db[collection].insert_many(data, ordered=False)
        n_inserted = len(res.inserted_ids)
        LOGGER.info(f'{collection}: nInserted={n_inserted}')
    except BulkWriteError as err:
        n_errors = len(err.details.get('writeErrors'))
        n_inserted = int(err.details.get('nInserted'))
        LOGGER.info(f'{collection}: nInserted={n_inserted}, writeErrors={n_errors}')
    except (AttributeError, TypeError) as err:
        n_inserted = -13
        LOGGER.error(err)
    finally:
        return n_inserted


def upsert_many(
        db: Database,
        collection: str,
        data: Iterable[dict[str, Any]]
) -> int:
    """MongoDB batch upsert (on conflict _id, do update). Return: number of upserted rows."""
    n_inserted = -1
    try:
        requests = [
            ReplaceOne({'id': rec.get('id', 0)}, rec, upsert=True)
            for rec in data
        ]
        res = db[collection].bulk_write(requests)
        n_inserted = len(res.upserted_ids)
        LOGGER.info(
            f'{collection}: nInserted={res.upserted_count}, nModified={res.modified_count}'
        )
    except BulkWriteError as err:
        n_errors = len(err.details.get('writeErrors', []))
        n_inserted = int(err.details.get('nInserted')) + int(err.details.get('nUpserted'))
        LOGGER.info(f'{collection}: nInserted={n_inserted}, writeErrors={n_errors}')
    except (AttributeError, TypeError) as err:
        n_inserted = -3
        LOGGER.error(err)
    finally:
        return n_inserted


def bulkwrite_handler(func):
    def wrapper(*args, **kwargs) -> dict[str, int]:
        collection = kwargs.get("collection", "collection")
        try:
            res: dict[str, int] = func(*args, **kwargs)
            LOGGER.info(
                f'{collection}: nInserted={res.get("nInserted", 0)}, ' +
                f'nModified={res.get("nModified", 0)}'
            )
            return res
        except BulkWriteError as err:
            n_errors = len(err.details.get('writeErrors', []))
            n_inserted = int(err.details.get('nInserted')) + int(err.details.get('nUpserted'))
            LOGGER.info(f'{collection}: nInserted={n_inserted}, writeErrors={n_errors}')
            return {"nInserted": n_inserted}
        except (AttributeError, TypeError) as err:
            LOGGER.error(err)

    return wrapper


@bulkwrite_handler
def bulk_insert(db: Database, collection: str, data: Iterable[dict[str, Any]]):
    """MongoDB bulk insert (on conflict _id, do nothing). Return: number of inserted rows."""
    res = db[collection].insert_many(data, ordered=False)
    return {"nInserted": len(res.inserted_ids)}


@bulkwrite_handler
def bulk_upsert(db: Database, collection: str, data: Iterable[dict[str, Any]]):
    """MongoDB bulk upsert (on conflict _id, do update). Return: number of upserted rows."""
    requests = [
        ReplaceOne({'id': rec.get('id', 0)}, rec, upsert=True)
        for rec in data
    ]
    res = db[collection].bulk_write(requests)
    return {
        "nInserted": int(res.upserted_count) + int(res.inserted_count),
        "nModified": int(res.modified_count)
    }
