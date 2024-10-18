import os
from celery import Celery, Task
from pymongo import MongoClient
from pymongo.database import Database
from .config import Config
from .spider.exchange import Exchange
from .spider.XTBApi import Client

app = Celery(
    __name__,
    broker=Config.REDIS_URI,
    backend=Config.REDIS_URI,
    include=['project.spider.tasks'],
    task_routes={
        "project.spider.tasks.collect_candles": {"queue": "pool_solo"},
        "project.spider.tasks.upsert_technical_analysis": {"queue": "pool_any"}
    },
    task_cls=Exchange
)


class XTBClientTask(Task):
    user: str = os.getenv('WORKER_ID', '')
    _client: Client | None = None

    @property
    def client(self):
        if not self._client:
            token = Exchange.ACCOUNTS.get(self.user, {}).get('pass', '')
            self._client = Client(self.user, token=token, mode='real')
            self._client.login()
        return self._client


class MongoDBTask(Task):
    db_name: str = Config.MONGODB_NAME
    _client: MongoClient | None = None
    _db: Database | None = None

    @property
    def db(self):
        if self._db is None:
            self._client = MongoClient(Config.MONGO_URI)
            self._db = self._client[self.db_name]
        return self._db


class CandleTask(XTBClientTask):
    symbol_ids: dict[str, int] = Exchange.SYMBOL_ID
    period_ids: dict[int, int] = Exchange.PERIOD_ID
    presets: dict[str, list] = Exchange.PRESETS


class TATask(MongoDBTask):
    presets: dict[str, list] = Exchange.PRESETS
