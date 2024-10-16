from celery.result import AsyncResult
from fastapi import FastAPI
from contextlib import asynccontextmanager
from .share.database import engine, mongo_conn
from .spider.models import Base
from .spider.route import router as SpiderRouter


@asynccontextmanager
async def mongodb_lifespan(_app: FastAPI):
    _app.mongodb = await mongo_conn()
    yield
    _app.mongodb.close()


Base.metadata.create_all(bind=engine)
app = FastAPI(lifespan=mongodb_lifespan)
app.include_router(SpiderRouter, tags=["Spider"], prefix="/candles")


@app.get("/", tags=["Root"])
async def read_root():
    """
    Developer's greeting
    """
    return {"message": "Welcome Home!"}


@app.get("/tasks/{task_id}")
def task_status(task_id: str):
    """
    Get task status.
    PENDING (waiting for execution or unknown task id)
    STARTED (task has been started)
    SUCCESS (task executed successfully)
    FAILURE (task execution resulted in exception)
    RETRY (task is being retried)
    REVOKED (task has been revoked)
    """
    task = AsyncResult(task_id)
    state = task.state

    if state == "FAILURE":
        error = str(task.result)
        response = {
            "state": state,
            "error": error,
        }
    else:
        response = {
            "state": state,
        }
    return response
