from contextlib import contextmanager
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config.settings import get_db_url

_engine: Optional[Engine] = None

def get_engine() -> Engine:
    """
    Returns a singleton SQLAlchemy engine with connection pooling.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            get_db_url(),
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )
    return _engine

@contextmanager
def get_db_connection():
    """
    Context manager providing a database connection with automatic commit/rollback.
    """
    engine = get_engine()
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()
