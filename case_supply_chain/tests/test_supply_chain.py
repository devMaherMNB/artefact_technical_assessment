import pytest
import pandas as pd
from sqlalchemy import text
from common.db_config import engine

def test_table_exists():
    """Verify that all core tables exist in the supply_chain schema."""
    tables = ['dim_products', 'dim_suppliers', 'dim_warehouses', 'dim_orders', 'dim_time', 'fact_shipments']
    with engine.connect() as conn:
        for table in tables:
            result = conn.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'supply_chain' AND table_name = '{table}');"
            ))
            assert result.scalar() is True, f"Table {table} does not exist."

def test_data_count():
    """Verify that the data generator actually populated the tables."""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM supply_chain.fact_shipments;"))
        count = result.scalar()
        assert count > 0, "fact_shipments table is empty."

def test_shipment_value_calculation():
    """Verify that shipment_value equals quantity * unit_price (approx)."""
    query = """
    SELECT fs.shipment_id, fs.quantity, dp.unit_price, fs.shipment_value
    FROM supply_chain.fact_shipments fs
    JOIN supply_chain.dim_products dp ON fs.product_id = dp.product_id
    LIMIT 10;
    """
    df = pd.read_sql(query, engine)
    for _, row in df.iterrows():
        # Using a small tolerance for floating point comparison if needed
        expected = round(row['quantity'] * float(row['unit_price']), 2)
        assert abs(float(row['shipment_value']) - expected) < 0.01

def test_audit_trigger_on_update():
    """Verify that updating a shipment value triggers an entry in shipment_audit_log."""
    with engine.begin() as conn:
        # 1. Get a shipment
        shipment = conn.execute(text("SELECT shipment_id, shipment_value FROM supply_chain.fact_shipments LIMIT 1;")).fetchone()
        s_id = shipment[0]
        old_val = float(shipment[1])
        new_val = old_val + 100.0

        # 2. Update it
        conn.execute(text(f"UPDATE supply_chain.fact_shipments SET shipment_value = {new_val} WHERE shipment_id = {s_id};"))

        # 3. Check audit log
        audit = conn.execute(text(f"SELECT operation, old_value, new_value FROM supply_chain.shipment_audit_log WHERE shipment_id = {s_id} ORDER BY changed_at DESC LIMIT 1;")).fetchone()
        
        assert audit is not None, "No audit log entry found."
        assert audit[0] == 'UPDATE'
        assert float(audit[1]) == old_val
        assert float(audit[2]) == new_val
