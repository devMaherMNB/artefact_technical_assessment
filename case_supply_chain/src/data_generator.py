import pandas as pd
import numpy as np
from faker import Faker
from sqlalchemy import text
from datetime import datetime, timedelta, date
from common.db_config import engine
import common.logger as logger  # This triggers the basicConfig in common/logger.py

logger = logger.getLogger("Supply Chain")

fake = Faker()


class SupplyChainGenerator:
    def __init__(self, num_products=50, num_suppliers=20, num_warehouses=10, num_orders=500, num_shipments=1000):
        self.num_products = num_products
        self.num_suppliers = num_suppliers
        self.num_warehouses = num_warehouses
        self.num_orders = num_orders
        self.num_shipments = num_shipments
        self.schema = "supply_chain"

    def truncate_tables(self):
        """Clears all tables before generating data."""
        tables = [
            'fact_shipments', 'dim_orders', 'dim_warehouses',
            'dim_suppliers', 'dim_products', 'dim_time'
        ]
        with engine.begin() as conn:
            for table in tables:
                conn.execute(text(f"TRUNCATE TABLE {self.schema}.{table} RESTART IDENTITY CASCADE;"))
        logger.info("All tables truncated and identities reset.")

    def generate_time_dimension(self, start_date="2024-01-01", end_date="2025-12-31"):
        """Populates the dim_time table for a given range."""
        logger.info(f"Generating time dimension from {start_date} to {end_date}")
        dates = pd.date_range(start=start_date, end=end_date)
        df = pd.DataFrame({
            'full_date': dates.date,
            'date_id': [int(d.strftime('%Y%m%d')) for d in dates],
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.month_name(),
            'week': dates.isocalendar().week,
            'day_of_week': dates.dayofweek + 1,
            'day_name': dates.day_name(),
            'is_weekend': dates.dayofweek >= 5
        })
        df.to_sql('dim_time', engine, schema=self.schema, if_exists='append', index=False)
        return df

    def generate_products(self):
        logger.info(f"Generating {self.num_products} products")
        categories = ['Electronics', 'Automotive', 'Industrial', 'Retail', 'Apparel']
        products = []
        for _ in range(self.num_products):
            products.append({
                'product_name': fake.catch_phrase(),
                'product_category': np.random.choice(categories),
                'sku': fake.unique.bothify(text='SKU-####-????'),
                'unit_price': round(np.random.uniform(10, 500), 2),
                'description': fake.sentence()
            })
        df = pd.DataFrame(products)
        df.to_sql('dim_products', engine, schema=self.schema, if_exists='append', index=False)
        return pd.read_sql(f"SELECT product_id, unit_price FROM {self.schema}.dim_products", engine)

    def generate_suppliers(self):
        logger.info(f"Generating {self.num_suppliers} suppliers")
        suppliers = []
        for _ in range(self.num_suppliers):
            suppliers.append({
                'supplier_name': fake.company(),
                'supplier_country': fake.country(),
                'contact_email': fake.company_email(),
                'phone': fake.phone_number(),
                'rating': round(np.random.uniform(1, 5), 1)
            })
        df = pd.DataFrame(suppliers)
        df.to_sql('dim_suppliers', engine, schema=self.schema, if_exists='append', index=False)
        return pd.read_sql(f"SELECT supplier_id FROM {self.schema}.dim_suppliers", engine)

    def generate_warehouses(self):
        logger.info(f"Generating {self.num_warehouses} warehouses")
        warehouses = []
        for _ in range(self.num_warehouses):
            warehouses.append({
                'warehouse_name': f"WH-{fake.city()}",
                'warehouse_location': fake.address().replace('\n', ', '),
                'warehouse_country': fake.country(),
                'capacity_sqft': np.random.randint(50000, 500000),
                'manager_name': fake.name()
            })
        df = pd.DataFrame(warehouses)
        df.to_sql('dim_warehouses', engine, schema=self.schema, if_exists='append', index=False)
        return pd.read_sql(f"SELECT warehouse_id FROM {self.schema}.dim_warehouses", engine)

    def generate_orders(self):
        logger.info(f"Generating {self.num_orders} orders")
        orders = []
        statuses = ['completed', 'pending', 'cancelled']
        priorities = ['high', 'medium', 'low']
        
        # Buffer of 10 days before end of 2025 for shipments
        start_date = date(2024, 1, 1)
        end_date = date(2025, 12, 21)
        
        for _ in range(self.num_orders):
            orders.append({
                'order_date': fake.date_between(start_date=start_date, end_date=end_date),
                'customer_id': fake.bothify(text='CUST-####'),
                'order_status': np.random.choice(statuses, p=[0.8, 0.15, 0.05]),
                'order_priority': np.random.choice(priorities)
            })
        df = pd.DataFrame(orders)
        df.to_sql('dim_orders', engine, schema=self.schema, if_exists='append', index=False)
        return pd.read_sql(f"SELECT order_id, order_date FROM {self.schema}.dim_orders", engine)

    def generate_shipments(self, products_df, suppliers_df, warehouses_df, orders_df):
        logger.info(f"Generating {self.num_shipments} shipments")
        shipments = []
        for _ in range(self.num_shipments):
            product = products_df.sample(1).iloc[0]
            order = orders_df.sample(1).iloc[0]
            
            shipment_date = order['order_date'] + timedelta(days=np.random.randint(1, 10))
            quantity = np.random.randint(1, 100)
            
            shipments.append({
                'product_id': int(product['product_id']),
                'supplier_id': int(suppliers_df.sample(1).iloc[0]['supplier_id']),
                'warehouse_id': int(warehouses_df.sample(1).iloc[0]['warehouse_id']),
                'order_id': int(order['order_id']),
                'date_id': int(shipment_date.strftime('%Y%m%d')),
                'shipment_date': shipment_date,
                'quantity': quantity,
                'shipment_value': round(quantity * float(product['unit_price']), 2),
                'shipping_time_hours': np.random.randint(12, 120)
            })
        df = pd.DataFrame(shipments)
        df.to_sql('fact_shipments', engine, schema=self.schema, if_exists='append', index=False)

    def run(self):
        """Main execution flow."""
        try:
            self.truncate_tables()
            self.generate_time_dimension()
            prod_df = self.generate_products()
            supp_df = self.generate_suppliers()
            wh_df = self.generate_warehouses()
            ord_df = self.generate_orders()
            self.generate_shipments(prod_df, supp_df, wh_df, ord_df)
            logger.info("Data generation completed successfully!")
        except Exception as e:
            logger.error(f"Error during data generation: {e}")
            raise

if __name__ == "__main__":
    generator = SupplyChainGenerator()
    generator.run()
