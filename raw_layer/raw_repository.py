import json
from typing import Any, Dict
from sqlalchemy import text
from config.database import get_db_connection

def save_raw_payload(business_id: int, source: str, data_type: str, payload: Dict[str, Any]) -> int:
    """
    Inserts unmodified API response JSON into raw.raw_data table in PostgreSQL.
    Returns the auto-generated raw_id.
    """
    payload_json = json.dumps(payload, ensure_ascii=False)
    
    query = text("""
        INSERT INTO raw.raw_data (business_id, source, data_type, payload, created_at)
        VALUES (:business_id, :source, :data_type, CAST(:payload AS JSONB), now())
        RETURNING raw_id;
    """)
    
    with get_db_connection() as conn:
        result = conn.execute(query, {
            "business_id": business_id,
            "source": source,
            "data_type": data_type,
            "payload": payload_json
        })
        raw_id = result.scalar()
        conn.commit()
        return raw_id
