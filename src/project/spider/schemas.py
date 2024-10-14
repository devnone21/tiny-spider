from typing import Optional
from pydantic import BaseModel
from datetime import date


class CandleBase(BaseModel):
    ctm: int
    ctmstring: Optional[str] = ''
    open: int | float
    close: int | float
    high: int | float
    low: int | float
    vol: int | float


class CandleIn(CandleBase):
    id: int
    symbol_id: Optional[int] = 0
    timeframe_id: Optional[int] = 0

    def as_tuple(self):
        return (
            self.id, self.symbol_id, self. timeframe_id, self.ctm, self.ctmstring,
            self.open, self.close, self.high, self.low, self.vol
        )


class CandleOut(CandleBase):
    pass


class CandleStatBase(BaseModel):
    symbol_id: int
    timeframe_id: int
    symbol: str
    period: int
    date_from: date
    date_until: date
    digits: int

    def as_tuple(self):
        return (
            self.id, self.symbol_id, self. timeframe_id, self.symbol, self.period,
            self.date_from, self.date_until, self.digits
        )
