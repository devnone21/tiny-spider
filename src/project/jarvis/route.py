from fastapi import APIRouter, HTTPException

from .tasks import collect_candles
from .crud import error_message, query_ct, get_candles

router = APIRouter()


@router.post("/{symbol}/{period}", response_description="Candles collection task to Workers")
def send_task_candles(symbol: str, period: int):
    task = collect_candles.delay(symbol, period)
    return {"task_id": task.id}


@router.get("/{symbol_id}/{period_id}", response_description="Candles sample from database")
def get_sample_candles(symbol_id: int, period_id: int):
    candles = get_candles(symbol_id, period_id)
    if not candles:
        raise HTTPException(404, error_message(f"Not found - S:{symbol_id}/T:{period_id}"))
    return candles


@router.get("/ct/{symbol_id}/{period_id}", response_description="Candles stats from database")
def get_ct(symbol_id: int, period_id: int):
    ct = query_ct(symbol_id, period_id)
    if not ct:
        raise HTTPException(404, error_message(f"Not found - S:{symbol_id}/T:{period_id}"))
    return ct
