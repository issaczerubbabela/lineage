"""
Sample data generator for the lineage project
Creates realistic datasets for demonstration purposes
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def generate_customers_data(num_records=10000):
    """Generate customer data"""
    customers = []
    
    for i in range(num_records):
        customer = {
            'customer_id': f"CUST_{i+1:06d}",
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'country': fake.country(),
            'age': random.randint(18, 80),
            'gender': random.choice(['M', 'F', 'Other']),
            'registration_date': fake.date_between(start_date='-2y', end_date='today'),
            'customer_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'is_active': np.random.choice([True, False], p=[0.8, 0.2])
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_products_data(num_records=1000):
    """Generate product data"""
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Beauty', 'Automotive']
    
    products = []
    
    for i in range(num_records):
        category = random.choice(categories)
        product = {
            'product_id': f"PROD_{i+1:06d}",
            'product_name': fake.catch_phrase(),
            'category': category,
            'subcategory': f"{category}_Sub_{random.randint(1, 5)}",
            'brand': fake.company(),
            'price': round(random.uniform(10, 1000), 2),
            'cost': 0,  # Will be calculated as percentage of price
            'weight': round(random.uniform(0.1, 50), 2),
            'dimensions': f"{random.randint(5, 50)}x{random.randint(5, 50)}x{random.randint(5, 50)}",
            'color': fake.color_name(),
            'material': random.choice(['Plastic', 'Metal', 'Wood', 'Glass', 'Fabric', 'Leather']),
            'stock_quantity': random.randint(0, 1000),
            'reorder_level': random.randint(10, 100),
            'supplier_id': f"SUPP_{random.randint(1, 100):03d}",
            'launch_date': fake.date_between(start_date='-3y', end_date='today'),
            'rating': round(random.uniform(1, 5), 1),
            'is_discontinued': np.random.choice([True, False], p=[0.1, 0.9])
        }
        
        # Calculate cost as 60-80% of price
        product['cost'] = round(product['price'] * random.uniform(0.6, 0.8), 2)
        
        products.append(product)
    
    return pd.DataFrame(products)

def generate_orders_data(num_records=50000, customer_df=None, product_df=None):
    """Generate order data"""
    if customer_df is None or product_df is None:
        raise ValueError("Customer and Product dataframes are required")
    
    customer_ids = customer_df['customer_id'].tolist()
    product_ids = product_df['product_id'].tolist()
    
    orders = []
    
    for i in range(num_records):
        order_date = fake.date_between(start_date='-1y', end_date='today')
        shipping_date = order_date + timedelta(days=random.randint(1, 7))
        
        order = {
            'order_id': f"ORD_{i+1:08d}",
            'customer_id': random.choice(customer_ids),
            'order_date': order_date,
            'shipping_date': shipping_date,
            'delivery_date': shipping_date + timedelta(days=random.randint(1, 14)),
            'order_status': random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'], 
                                        weights=[0.1, 0.2, 0.6, 0.1]),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
            'shipping_method': random.choice(['Standard', 'Express', 'Overnight']),
            'shipping_cost': round(random.uniform(5, 50), 2),
            'tax_amount': 0,  # Will be calculated
            'discount_amount': round(random.uniform(0, 100), 2),
            'total_amount': 0,  # Will be calculated
            'currency': 'USD',
            'sales_channel': random.choice(['Online', 'Store', 'Phone', 'Mobile App'])
        }
        orders.append(order)
    
    return pd.DataFrame(orders)

def generate_order_items_data(order_df=None, product_df=None):
    """Generate order items data"""
    if order_df is None or product_df is None:
        raise ValueError("Order and Product dataframes are required")
    
    order_items = []
    
    for _, order in order_df.iterrows():
        # Each order has 1-5 items
        num_items = random.randint(1, 5)
        selected_products = random.sample(product_df['product_id'].tolist(), 
                                        min(num_items, len(product_df)))
        
        order_total = 0
        
        for j, product_id in enumerate(selected_products):
            product_info = product_df[product_df['product_id'] == product_id].iloc[0]
            quantity = random.randint(1, 3)
            unit_price = product_info['price']
            line_total = unit_price * quantity
            order_total += line_total
            
            item = {
                'order_item_id': f"{order['order_id']}_ITEM_{j+1:02d}",
                'order_id': order['order_id'],
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': line_total,
                'discount_percent': random.uniform(0, 20),
                'tax_percent': 8.5
            }
            order_items.append(item)
        
        # Update order total (simplified calculation)
        order_df.loc[order_df['order_id'] == order['order_id'], 'total_amount'] = order_total
    
    return pd.DataFrame(order_items)

def generate_inventory_data(product_df=None):
    """Generate inventory tracking data"""
    if product_df is None:
        raise ValueError("Product dataframe is required")
    
    inventory = []
    
    for _, product in product_df.iterrows():
        # Generate multiple inventory records per product
        for i in range(random.randint(5, 20)):
            transaction_date = fake.date_between(start_date='-6m', end_date='today')
            
            inventory_record = {
                'inventory_id': f"INV_{len(inventory)+1:08d}",
                'product_id': product['product_id'],
                'transaction_date': transaction_date,
                'transaction_type': random.choice(['Purchase', 'Sale', 'Adjustment', 'Return']),
                'quantity_change': random.randint(-100, 200),
                'unit_cost': product['cost'],
                'location': random.choice(['Warehouse A', 'Warehouse B', 'Store 1', 'Store 2']),
                'batch_number': f"BATCH_{random.randint(1000, 9999)}",
                'expiry_date': fake.date_between(start_date='today', end_date='+2y') if random.random() > 0.7 else None,
                'supplier_id': product['supplier_id']
            }
            inventory.append(inventory_record)
    
    return pd.DataFrame(inventory)

def save_sample_data():
    """Generate and save all sample datasets"""
    print("Generating sample data...")
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Generate datasets
    print("Generating customers data...")
    customers_df = generate_customers_data(10000)
    customers_df.to_csv(os.path.join(data_dir, 'customers.csv'), index=False)
    
    print("Generating products data...")
    products_df = generate_products_data(1000)
    products_df.to_csv(os.path.join(data_dir, 'products.csv'), index=False)
    
    print("Generating orders data...")
    orders_df = generate_orders_data(50000, customers_df, products_df)
    orders_df.to_csv(os.path.join(data_dir, 'orders.csv'), index=False)
    
    print("Generating order items data...")
    order_items_df = generate_order_items_data(orders_df, products_df)
    order_items_df.to_csv(os.path.join(data_dir, 'order_items.csv'), index=False)
    
    print("Generating inventory data...")
    inventory_df = generate_inventory_data(products_df)
    inventory_df.to_csv(os.path.join(data_dir, 'inventory.csv'), index=False)
    
    print("Sample data generation completed!")
    print(f"Generated files in {data_dir}:")
    print(f"- customers.csv: {len(customers_df)} records")
    print(f"- products.csv: {len(products_df)} records")
    print(f"- orders.csv: {len(orders_df)} records")
    print(f"- order_items.csv: {len(order_items_df)} records")
    print(f"- inventory.csv: {len(inventory_df)} records")

if __name__ == "__main__":
    save_sample_data()
