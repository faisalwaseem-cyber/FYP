from sqlalchemy import text
from typing import Optional
from config.database import get_db_connection

class SyncLogger:
    """
    Manages operational logs inside raw.sync_log table.
    Tracks execution status, volume of processed records, and failures.
    """
    def __init__(self, business_id: int, source: str = "shopify"):
        self.business_id = business_id
        self.source = source
        self.sync_id: Optional[int] = None

    def start_sync(self, sync_type: str = "incremental") -> int:
        query = text("""
            INSERT INTO raw.sync_log (business_id, source, sync_type, status, started_at)
            VALUES (:b_id, :source, :sync_type, 'started', now())
            RETURNING sync_id;
        """)
        with get_db_connection() as conn:
            result = conn.execute(query, {
                "b_id": self.business_id,
                "source": self.source,
                "sync_type": sync_type
            })
            self.sync_id = result.scalar()
            conn.commit()
            return self.sync_id

    def finish_success(self, fetched: int, inserted: int, updated: int):
        if not self.sync_id:
            return
        query = text("""
            UPDATE raw.sync_log
            SET status = 'success',
                records_fetched = :fetched,
                records_inserted = :inserted,
                records_updated = :updated,
                completed_at = now()
            WHERE sync_id = :sync_id;
        """)
        with get_db_connection() as conn:
            conn.execute(query, {
                "fetched": fetched,
                "inserted": inserted,
                "updated": updated,
                "sync_id": self.sync_id
            })
            conn.commit()

    def finish_failure(self, error_msg: str):
        if not self.sync_id:
            return
        query = text("""
            UPDATE raw.sync_log
            SET status = 'failed',
                error_message = :err,
                completed_at = now()
            WHERE sync_id = :sync_id;
        """)
        with get_db_connection() as conn:
            conn.execute(query, {
                "err": str(error_msg),
                "sync_id": self.sync_id
            })
            conn.commit()
