import pandas as pd
import pytest
from case_online_retail.src.transform import run_transform
from unittest.mock import patch

def test_duplicates_dropped():
    # Test 1 — Duplicates are dropped
    data = {
        'invoice_no': ['1', '1', '2'],
        'stock_code': ['A', 'A', 'B'],
        'unit_price': [1.0, 1.0, 2.0],
        'quantity': [1, 1, 2],
        'customer_id': ['C1', 'C1', 'C2'],
        'description': ['D1', 'D1', 'D2'],
        'invoice_date': ['2010-12-01', '2010-12-01', '2010-12-01'],
        'country': ['UK', 'UK', 'UK']
    }
    df_input = pd.DataFrame(data)

    with patch('pandas.read_sql', return_value=df_input), \
         patch('sqlalchemy.create_engine'):
        _, _, df_facts = run_transform()
        # Initial 3 rows, 2 are identical, should result in 2 rows
        assert len(df_facts) == 2

def test_null_customer_filled():
    # Test 2 — Null customer_id is filled with 'UNKNOWN'
    data = {
        'invoice_no': ['1'],
        'stock_code': ['A'],
        'unit_price': [1.0],
        'quantity': [1],
        'customer_id': [None],
        'description': ['D1'],
        'invoice_date': ['2010-12-01'],
        'country': ['UK']
    }
    df_input = pd.DataFrame(data)

    with patch('pandas.read_sql', return_value=df_input), \
         patch('sqlalchemy.create_engine'):
        _, _, df_facts = run_transform()
        assert df_facts.iloc[0]['raw_customer_id'] == 'UNKNOWN'

def test_unit_price_filtering():
    # Test 3 — unit_price <= 0 rows are filtered out
    data = {
        'invoice_no': ['1', '2', '3'],
        'stock_code': ['A', 'B', 'C'],
        'unit_price': [1.5, 0, -2],
        'quantity': [1, 1, 1],
        'customer_id': ['C1', 'C2', 'C3'],
        'description': ['D1', 'D2', 'D3'],
        'invoice_date': ['2010-12-01', '2010-12-01', '2010-12-01'],
        'country': ['UK', 'UK', 'UK']
    }
    df_input = pd.DataFrame(data)

    with patch('pandas.read_sql', return_value=df_input), \
         patch('sqlalchemy.create_engine'):
        _, _, df_facts = run_transform()
        # Only the row with 1.5 should remain
        assert len(df_facts) == 1
        assert df_facts.iloc[0]['unit_price'] == 1.5

def test_total_value_calculation():
    # Test 4 — total_value is computed correctly
    data = {
        'invoice_no': ['1'],
        'stock_code': ['A'],
        'unit_price': [2.50],
        'quantity': [3],
        'customer_id': ['C1'],
        'description': ['D1'],
        'invoice_date': ['2010-12-01'],
        'country': ['UK']
    }
    df_input = pd.DataFrame(data)

    with patch('pandas.read_sql', return_value=df_input), \
         patch('sqlalchemy.create_engine'):
        _, _, df_facts = run_transform()
        # 3 * 2.50 = 7.50
        assert df_facts.iloc[0]['total_value'] == 7.50

def test_date_id_derivation():
    # Test 5 — date_id is derived correctly
    data = {
        'invoice_no': ['1'],
        'stock_code': ['A'],
        'unit_price': [1.0],
        'quantity': [1],
        'customer_id': ['C1'],
        'description': ['D1'],
        'invoice_date': ['2010-12-01 08:26:00'],
        'country': ['UK']
    }
    df_input = pd.DataFrame(data)

    with patch('pandas.read_sql', return_value=df_input), \
         patch('sqlalchemy.create_engine'):
        _, _, df_facts = run_transform()
        # '2010-12-01' -> 20101201
        assert df_facts.iloc[0]['date_id'] == 20101201
