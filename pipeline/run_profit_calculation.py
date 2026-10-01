"""Recalculate order profitability for the specified (or configured) business."""
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from analytics.profit_engine import calculate_order_profitability
from config.settings import BUSINESS_ID


def run_profit_calculation(business_id: int = BUSINESS_ID) -> int:
    """
    Recalculates order profitability for the given business.

    Parameters
    ----------
    business_id : int
        Tenant identifier. Defaults to the value in settings / .env.

    Returns
    -------
    int
        Number of orders whose profit_calculation rows were affected.
    """
    affected_rows = calculate_order_profitability(business_id)
    print(
        f"[SUCCESS] Profitability calculated for {affected_rows} orders "
        f"(business_id={business_id})."
    )
    return affected_rows


if __name__ == "__main__":
    run_profit_calculation(BUSINESS_ID)
