import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from contextlib import contextmanager
from config import Config

DATABASE = Config.PGSQL_URI

engine = create_engine(DATABASE)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


@contextmanager
def db_session() -> Session:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def db_conn() -> psycopg2._psycopg.connection:
    conn = psycopg2.connect(DATABASE)
    try:
        yield conn
    finally:
        conn.close()
