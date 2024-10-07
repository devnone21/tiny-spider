from datetime import datetime, date, timedelta, timezone
from celery import Celery, Task
from celery.app import task
from celery.signals import celeryd_init
from redis import Redis
from config import Config
from database import DATABASE
from exchange import ExchangeData
from schemas import CandleIn, CandleStatBase
from crud import query_ct, insert_ct, update_ct, upsert_many_candles
from crud import gather_present_candles, gather_olden_candles
from XTBApi import Client
import logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

app = Celery(
    "tasks",
    broker=Config.REDIS_URI,
    backend=f"db+{DATABASE}",
)

SESSION_POOL_FIFO = "XTBSessionPool"
redis_conn = Redis(Config.REDIS_HOST, decode_responses=True)


def _extract_stream_info(sdata: list) -> tuple[str, dict]:
    # sdata = [
    #     [
    #         'STREAM_KEY',
    #         [
    #             ('1728187084376-1', {'user': '12345678', 'password': 's3cr3t'})
    #         ]
    #      ]
    # ]
    entry_id, info = sdata[0][1][0]
    return entry_id, info


class XTBSessionTask(Task):
    info: dict = {}
    mode: str = 'real'
    last_used: int = 0
    _client: Client | None = None
    LOGGER = logging.getLogger(__name__)

    @property
    def client(self):
        if not self._client:
            self._client = self.get_fifo()
        return self._client

    def get_fifo(self):
        res: list = redis_conn.xread(streams={SESSION_POOL_FIFO: 0}, count=1, block=200)
        if not res:
            self.LOGGER.info("FIFO: xread found nothing")
            return

        # try claim client
        client_id, info = _extract_stream_info(res)
        res = redis_conn.xdel(SESSION_POOL_FIFO, client_id)
        if not res:
            self.LOGGER.info("FIFO: xdel unable to claim")
            return
        session_ts = client_id.split("-")[0]
        self.last_used = int(session_ts)
        self.info = info

        # init client
        self._client = Client(info['user'], info['token'], self.mode)
        res: dict = self._client.login()
        if not res.get('status', False):
            self.LOGGER.info(f"FIFO: login failed client {info['user']}")
            self.reclaim()
            return

        # provide client
        self.LOGGER.info(f"FIFO: provide client {info['user']}")
        return self._client

    def reclaim(self):
        """reclaim the client back to Redis stream"""
        if self._client:
            self._client = None
        self.LOGGER.info(f"FIFO: xadd reclaim {self.info['user']}")
        return redis_conn.xadd(SESSION_POOL_FIFO, self.info)


@celeryd_init.connect
def init_session_pool_fifo(**kwargs):
    """At Celery start, initial Redis FIFO Stream of session pool"""
    stream_key = SESSION_POOL_FIFO
    redis_conn.delete(stream_key)
    accounts = Config.EXAPI_USER.split(',')
    for user in accounts:
        token = ExchangeData.ACCOUNTS.get(user, {}).get('pass', '')
        redis_conn.xadd(stream_key, {"user": user, "token": token})
        LOGGER.info(f"FIFO: xadd init {user}")
    return kwargs


@app.task(base=XTBSessionTask, bind=True)
def collect_candles(self: task, symbol: str, period: int):
    """Worker task to collect candles by symbol & period"""

    # get candles stats
    today_utc = datetime.now(timezone.utc).date()
    _ct = CandleStatBase(
        symbol_id=ExchangeData.SYMBOL_ID.get(symbol),
        timeframe_id=ExchangeData.PERIOD_ID.get(period),
        symbol=symbol, period=period,
        date_from=today_utc, date_until=today_utc
    )
    ct = query_ct(_ct.symbol_id, _ct.timeframe_id)
    if not ct:
        ct = insert_ct(_ct)

    # gather candles
    candles = gather_present_candles(self.client, ct)
    olden_candles = gather_olden_candles(self.client, ct)
    candles.extend(olden_candles)

    # return if no new candles
    if not candles:
        return

    # store new candles in DB
    model_candles = [CandleIn(
        id=ct.symbol_id * 10 + ct.timeframe_id + candle['ctm'],
        symbol_id=ct.symbol_id,
        timeframe_id=ct.timeframe_id,
        ctm=candle['ctm'],
        ctmstring=candle['ctmString'],
        open=candle['open'],
        close=candle['close'],
        high=candle['high'],
        low=candle['low'],
        vol=candle['vol']) for candle in candles]
    rowcount = upsert_many_candles(model_candles)

    # update candles stats
    olden_ts = 0 if not olden_candles else min([int(c['ctm']) for c in olden_candles]) / 1000
    if rowcount >= 0 and olden_ts > datetime(2020, 7, 1).timestamp():
        ct.date_from = date.fromtimestamp(olden_ts) + timedelta(days=1)
    present_ts = max([int(c['ctm']) for c in candles]) / 1000
    ct.date_until = date.fromtimestamp(present_ts)
    update_ct(ct)
    return {"client": {
        "user": self._client._user,
        "ws": str(self._client.ws.socket),
    }}
