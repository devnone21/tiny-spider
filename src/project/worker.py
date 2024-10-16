from celery import Celery
from .share.config import Config

app = Celery(
    "worker",
    broker=Config.REDIS_URI,
    backend=Config.REDIS_URI,
    include=['project.spider.tasks']
)
