from celery.result import AsyncResult
from fastapi import FastAPI, HTTPException
from .share.database import engine
from .spider.models import Base
from .spider import crud
from .tasks import collect_candles

Base.metadata.create_all(bind=engine)

app = FastAPI()


@app.get("/")
def read_root():
    """
    Developer's greeting
    """
    return {"Tiny": "Hello world"}


@app.post("/collect-candles/{symbol}/{period}", status_code=201)
def send_task_candles(symbol: str, period: int):
    """
    Send task 'collect_candles' to Celery worker.
    Uses Redis as Broker and Postgres as Backend.
    """
    task = collect_candles.delay(symbol, period)
    return {"task_id": task.id}


@app.get("/sample-candles/{symbol_id}/{timeframe_id}")
def get_sample_candles(symbol_id: int, timeframe_id: int):
    """
    Get sample of candles from database.
    """
    candles = crud.get_candles(symbol_id, timeframe_id)
    if not candles:
        raise HTTPException(404, crud.error_message(f"Not found - S:{symbol_id}/T:{timeframe_id}"))
    return candles


@app.get("/ct/{symbol_id}/{timeframe_id}")
def get_ct(symbol_id: int, timeframe_id: int):
    """
    Get stat of the symbol from database.
    """
    ct = crud.query_ct(symbol_id, timeframe_id)
    if not ct:
        raise HTTPException(404, crud.error_message(f"Not found - S:{symbol_id}/T:{timeframe_id}"))
    return ct


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
