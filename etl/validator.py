import re
import pandas as pd
from typing import List, Dict, Any

class DataValidator:
    """
    Validates cleaned DataFrames against database constraints before attempting SQL insertion.
    """
    PHONE_REGEX = re.compile(r"^\+92[0-9]{10}$")
    ALLOWED_ORDER_STATUSES = {'pending', 'confirmed', 'fulfilled', 'delivered', 'cancelled', 'returned', 'rto'}
    ALLOWED_PAYMENT_METHODS = {'cod', 'card', 'bank_transfer', 'wallet', 'other'}

    @classmethod
    def validate_customers(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
            
        # Ensure required columns
        for col in ["business_id", "phone"]:
            if df[col].isnull().any():
                raise ValueError(f"Constraint violation: {col} cannot be null in customer data.")

        # Ensure phone adheres to regex
        invalid_phones = df[~df["phone"].str.match(cls.PHONE_REGEX, na=False)]
        if not invalid_phones.empty:
            raise ValueError(f"Invalid Pakistani phone numbers detected: {invalid_phones['phone'].tolist()[:5]}")

        return df

    @classmethod
    def validate_products(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Non-negative prices
        if (df["selling_price"] < 0).any() or (df["unit_cost"] < 0).any():
            raise ValueError("Constraint violation: Product prices/costs cannot be negative.")

        return df

    @classmethod
    def validate_orders(cls, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        # Non-negative amounts
        amount_cols = ["subtotal_amount", "discount_amount", "shipping_charged", "total_amount"]
        for col in amount_cols:
            if (df[col] < 0).any():
                raise ValueError(f"Constraint violation: {col} cannot be negative.")

        # Status validation
        invalid_statuses = set(df["order_status"]) - cls.ALLOWED_ORDER_STATUSES
        if invalid_statuses:
            raise ValueError(f"Invalid order statuses detected: {invalid_statuses}")

        # Payment method validation
        invalid_methods = set(df["payment_method"]) - cls.ALLOWED_PAYMENT_METHODS
        if invalid_methods:
            raise ValueError(f"Invalid payment methods detected: {invalid_methods}")

        return df
