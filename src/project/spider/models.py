from ..share.database import Base
from sqlalchemy import Column, String, Integer, BigInteger, Numeric, Date


class Candle(Base):
    __tablename__ = "candles"
    id = Column("id", BigInteger, primary_key=True)
    symbol_id = Column("symbol_id", Integer, index=True)
    timeframe_id = Column("timeframe_id", Integer, index=True)
    ctm = Column("ctm", BigInteger)
    ctmstring = Column("ctmstring", String)
    open = Column("open", Numeric)
    close = Column("close", Numeric)
    high = Column("high", Numeric)
    low = Column("low", Numeric)
    vol = Column("vol", Numeric)


class CandleStat(Base):
    __tablename__ = "candles_stat"
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    symbol_id = Column("symbol_id", Integer)
    timeframe_id = Column("timeframe_id", Integer)
    symbol = Column("symbol", String)
    period = Column("period", Integer)
    date_from = Column("date_from", Date)
    date_until = Column("date_until", Date)
