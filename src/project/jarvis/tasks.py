from datetime import datetime, date, timedelta, timezone
from celery.app import task
from pandas import DataFrame
from pandas_ta import Strategy
from ..worker import app
from .exchange import ExchangeData, XTBClientTask
from .schemas import CandleIn, CandleStatBase
from .crud import (
    query_ct, insert_ct, update_ct, upsert_many_candles,
    gather_present_candles, gather_olden_candles
)
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


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

    # create task technical analysis
    ct.digits = digits
    next_task = technical_analysis.delay(candles, symbol_id, period_id, digits)

    # update candles stats - date_from
    olden_ts = 0 if not olden_candles else min([int(c['ctm']) for c in olden_candles]) / 1000
    if rowcount >= 0 and olden_ts > datetime(2020, 7, 1).timestamp():
        ct.date_from = date.fromtimestamp(olden_ts) + timedelta(days=1)
    # update candles stats - date_until
    if candles:
        present_ts = max([int(c['ctm']) for c in candles]) / 1000
        ct.date_until = date.fromtimestamp(present_ts)

    update_ct(ct)

    return {
        "client": {
            "user": self.user,
            "ws": str(self._client.ws.socket),
        },
        "task_id": next_task.id,
    }


@app.task
def technical_analysis(rate_infos: list, symbol_id: int, period_id: int, digits: int):
    if not rate_infos:
        return {"result": "No Data"}

    rate_infos.sort(key=lambda by: by['ctm'])
    candles = DataFrame(rate_infos)
    candles['close'] = (candles['open'] + candles['close']) / 10 ** digits
    candles['high'] = (candles['open'] + candles['high']) / 10 ** digits
    candles['low'] = (candles['open'] + candles['low']) / 10 ** digits
    candles['open'] = candles['open'] / 10 ** digits

    result = {}
    for name, tech in ExchangeData.PRESETS.items():
        df = candles.copy()
        df.ta.strategy(Strategy(name=name, ta=tech))
        df.dropna(inplace=True, ignore_index=True)
        df['id'] = symbol_id * 10 + period_id + df['ctm']
        result[name] = df.tail(1).to_json(orient="index")

    return result
