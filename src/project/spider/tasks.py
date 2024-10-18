from datetime import datetime, date, timedelta, timezone
from celery.app import task
from celery.schedules import crontab
from pymongo.database import Database
from pandas import DataFrame
from pandas_ta import Strategy

from ..worker import app, CandleTask, TATask
from .exchange import Exchange
from .schemas import CandleIn, CandleStatBase
from .crud import (
    query_ct, insert_ct, update_ct, upsert_many_candles,
    gather_present_candles, gather_olden_candles,
    bulk_upsert
)


@app.on_after_configure.connect
def setup_cron_tasks(sender, **kwargs):
    # Execute every 15 minutes, on Workdays.
    for symbol, period in Exchange.SYMBOL_DEFAULT + Exchange.SYMBOL_SUBSCRIBE:
        sender.add_periodic_task(
            crontab(minute='*/15', hour='*', day_of_week='mon-fri'),
            collect_candles.s(
                args=(symbol, period),
                queue='pool_solo'
            )
        )


@app.task(base=CandleTask, bind=True)
def collect_candles(self: task, symbol: str, period: int):
    """Worker task to collect candles by symbol & period"""

    symbol_id: int = self.symbol_ids.get(symbol)
    period_id: int = self.period_ids.get(period)

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

    # return if nothing new
    if not candles and not olden_candles:
        return

    # create task technical analysis
    for name, _ in self.presets.items():
        upsert_technical_analysis.apply_async(
            args=(name, symbol_id, period_id, digits, candles),
            queue='pool_any'
        )

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


@app.task(base=TATask, bind=True)
def upsert_technical_analysis(
        self: task,
        strategy: str,
        symbol_id: int,
        period_id: int,
        digits: int,
        candles: list,
):
    """Worker task to upsert TA results by symbol & period"""

    db: Database = self.db
    presets: dict[str, list] = self.presets

    # prepare data
    candles.sort(key=lambda by: by['ctm'])
    df = DataFrame(candles)
    df['close'] = (df['open'] + df['close']) / 10 ** digits
    df['high'] = (df['open'] + df['high']) / 10 ** digits
    df['low'] = (df['open'] + df['low']) / 10 ** digits
    df['open'] = df['open'] / 10 ** digits
    # apply strategy
    ta = presets.get(strategy, [])
    df.ta.strategy(Strategy(name=strategy, ta=ta))
    df.dropna(inplace=True, ignore_index=True)
    # filter columns
    cols = df.columns.to_list()
    dropped_cols = ['ctm', 'ctmString', 'open', 'close', 'high', 'low', 'vol']
    selected_cols = [c for c in cols if c not in dropped_cols]
    # additional columns
    df['symbol_code'] = symbol_id * 10 + period_id
    df['id'] = df['ctm'] + symbol_id * 10 + period_id
    df['last_update'] = datetime.now(timezone.utc)
    # final columns
    final_cols = ['id', 'symbol_code'] + selected_cols + ['last_update']
    res = bulk_upsert(
            db, collection=strategy,
            data=df[final_cols].to_dict(orient='records')
    )
    return {
        "nInserted": res.get("nInserted", 0),
        "symbol": symbol_id * 10 + period_id,
        "strategy": strategy,
    }
