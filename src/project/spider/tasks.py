from datetime import datetime, date, timedelta, timezone
from celery.app import task
from pymongo.database import Database
from pandas import DataFrame
from pandas_ta import Strategy

from ..worker import app, XTBClientTask, MongoDBTask
from .exchange import ExchangeData
from .schemas import CandleIn, CandleStatBase
from .crud import (
    query_ct, insert_ct, update_ct, upsert_many_candles,
    gather_present_candles, gather_olden_candles,
    upsert_many
)


@app.task(base=XTBClientTask, bind=True)
def collect_candles(self: task, symbol: str, period: int):
    """Worker task to collect candles by symbol & period"""

    symbol_id = ExchangeData.SYMBOL_ID.get(symbol)
    period_id = ExchangeData.PERIOD_ID.get(period)

    # get candles stats
    today_utc = datetime.now(timezone.utc).date()
    _ct = CandleStatBase(
        symbol_id=symbol_id,
        timeframe_id=period_id,
        symbol=symbol, period=period, digits=0,
        date_from=today_utc, date_until=today_utc
    )
    ct = query_ct(symbol_id, period_id)
    if not ct:
        ct = insert_ct(_ct)

    # gather candles
    candles, digits = gather_present_candles(ct, self.client)
    olden_candles, _ = gather_olden_candles(ct, self.client)
    # candles.extend(olden_candles)

    # return if nothing new
    if not candles and not olden_candles:
        return

    # create task technical analysis
    for name, tech in ExchangeData.PRESETS.items():
        ta_task_send = upsert_technical_analysis.apply_async(
            args=(name, tech, candles, symbol_id, period_id, digits),
            queue='pool_any'
        )
    # ta_task_send = technical_analysis.apply_async(
    #     args=(candles, symbol_id, period_id, digits),
    #     queue='pool_any'
    # )

    # store new candles in DB
    model_candles = [CandleIn(
        id=symbol_id * 10 + period_id + candle['ctm'],
        symbol_id=symbol_id,
        timeframe_id=period_id,
        ctm=candle['ctm'],
        ctmstring=candle['ctmString'],
        open=candle['open'],
        close=candle['close'],
        high=candle['high'],
        low=candle['low'],
        vol=candle['vol']) for candle in candles + olden_candles]
    rowcount = upsert_many_candles(model_candles)

    # update candles stats - date_from
    olden_ts = 0 if not olden_candles else min([int(c['ctm']) for c in olden_candles]) / 1000
    if rowcount >= 0 and olden_ts > datetime(2020, 7, 1).timestamp():
        ct.date_from = date.fromtimestamp(olden_ts) + timedelta(days=1)
    # update candles stats - date_until
    if candles:
        present_ts = max([int(c['ctm']) for c in candles]) / 1000
        ct.date_until = date.fromtimestamp(present_ts)

    ct.digits = digits
    update_ct(ct)

    return {
        "client": {
            "user": self.user,
            "ws": str(self._client.ws.socket),
        },
    }


# @app.task   # (base=MongoDBTask, bind=True)
# def technical_analysis(
#         # self: task,
#         rate_infos: list,
#         symbol_id: int,
#         period_id: int,
#         digits: int
# ) -> dict[str, int]:
#     # db: Database = self.db
#     if not rate_infos:
#         return {"input": 0}
#
#     rate_infos.sort(key=lambda by: by['ctm'])
#     candles = DataFrame(rate_infos)
#     candles['close'] = (candles['open'] + candles['close']) / 10 ** digits
#     candles['high'] = (candles['open'] + candles['high']) / 10 ** digits
#     candles['low'] = (candles['open'] + candles['low']) / 10 ** digits
#     candles['open'] = candles['open'] / 10 ** digits
#
#     for name, tech in ExchangeData.PRESETS.items():
#         upsert_ta_task_send = upsert_many_ta.apply_async(
#             args=(name, tech, candles, symbol_id, period_id),
#             queue='pool_any'
#         )

    # def upsert_many_ta(name: str, tech: list[dict]) -> int:
    #     df = candles.copy()
    #     # fx
    #     df.ta.strategy(Strategy(name=name, ta=tech))
    #     df.dropna(inplace=True, ignore_index=True)
    #     # filter columns
    #     cols = df.columns.to_list()
    #     dropped_cols = ['ctm', 'ctmString', 'open', 'close', 'high', 'low', 'vol']
    #     selected_cols = [c for c in cols if c not in dropped_cols]
    #     # additional columns
    #     df['_id'] = symbol_id * 10 + period_id + df['ctm']
    #     df['last_update'] = datetime.now(timezone.utc)
    #     # final columns
    #     final_cols = ['_id'] + selected_cols + ['last_update']
    #     return upsert_many(db, collection=name, data=df[final_cols].to_dict(orient='records'))

    # return {
    #     name: upsert_many_ta(name, tech)
    #     for name, tech in ExchangeData.PRESETS.items()
    # }


@app.task(base=MongoDBTask, bind=True)
def upsert_technical_analysis(
        self: task,
        strategy: str, ta: list[dict],
        candles: list,
        symbol_id: int,
        period_id: int,
        digits: int,
):
    db: Database = self.db
    # prepare data
    candles.sort(key=lambda by: by['ctm'])
    df = DataFrame(candles)
    df['close'] = (df['open'] + df['close']) / 10 ** digits
    df['high'] = (df['open'] + df['high']) / 10 ** digits
    df['low'] = (df['open'] + df['low']) / 10 ** digits
    df['open'] = df['open'] / 10 ** digits
    # apply strategy
    df.ta.strategy(Strategy(name=strategy, ta=ta))
    df.dropna(inplace=True, ignore_index=True)
    # filter columns
    cols = df.columns.to_list()
    dropped_cols = ['ctm', 'ctmString', 'open', 'close', 'high', 'low', 'vol']
    selected_cols = [c for c in cols if c not in dropped_cols]
    # additional columns
    df['_id'] = symbol_id * 10 + period_id + df['ctm']
    df['last_update'] = datetime.now(timezone.utc)
    # final columns
    final_cols = ['_id'] + selected_cols + ['last_update']
    n_inserted = upsert_many(
            db, collection=strategy,
            data=df[final_cols].to_dict(orient='records')
    )
    return {
        "symbol": symbol_id * 10 + period_id,
        "strategy": strategy,
        "nInserted": n_inserted,
    }
