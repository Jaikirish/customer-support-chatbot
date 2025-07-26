import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
import os
import logging
from datetime import datetime
import sys

# Import models and database
from database import engine, SessionLocal, Base
from models import Product, Order, Inventory

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_dataframe(df):
    """Clean DataFrame by handling missing values and data types"""
    # Replace NaN with None for SQLAlchemy
    df = df.replace({np.nan: None})
    
    # Convert datetime columns
    for col in df.columns:
        if df[col].dtype == 'object':
            # Try to convert to datetime
            try:
                pd.to_datetime(df[col], errors='coerce')
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except:
                pass
    
    return df

def load_products_bulk(csv_path, session):
    """Load products data with bulk operations and duplicate handling"""
    try:
        logger.info(f"Loading products from {csv_path}")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        df = clean_dataframe(df)
        
        # Prepare bulk insert data
        products_data = []
        for _, row in df.iterrows():
            product_dict = {
                'id': row.get('id'),
                'name': row.get('name'),
                'category': row.get('category'),
                'price': row.get('price'),
                'quantity_sold': row.get('quantity_sold', 0)
            }
            products_data.append(product_dict)
        
        # Bulk insert with duplicate handling
        try:
            session.bulk_insert_mappings(Product, products_data)
            session.commit()
            logger.info(f"Successfully loaded {len(products_data)} products")
        except IntegrityError as e:
            logger.warning(f"Duplicate products found, using upsert approach: {e}")
            session.rollback()
            
            # Handle duplicates by checking existing records
            for product_data in products_data:
                try:
                    # Check if product exists
                    existing = session.query(Product).filter(Product.id == product_data['id']).first()
                    if existing:
                        # Update existing product
                        for key, value in product_data.items():
                            setattr(existing, key, value)
                    else:
                        # Insert new product
                        product = Product(**product_data)
                        session.add(product)
                except Exception as e:
                    logger.error(f"Error processing product {product_data.get('id')}: {e}")
                    continue
            
            session.commit()
            logger.info("Products loaded with duplicate handling")
            
    except Exception as e:
        logger.error(f"Error loading products: {e}")
        session.rollback()
        raise

def load_orders_bulk(csv_path, session):
    """Load orders data with bulk operations"""
    try:
        logger.info(f"Loading orders from {csv_path}")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        df = clean_dataframe(df)
        
        # Prepare bulk insert data
        orders_data = []
        for _, row in df.iterrows():
            order_dict = {
                'id': row.get('id'),
                'user_id': row.get('user_id'),
                'order_date': row.get('order_date', datetime.utcnow()),
                'status': row.get('status', 'pending'),
                'product_id': row.get('product_id')
            }
            orders_data.append(order_dict)
        
        # Bulk insert
        session.bulk_insert_mappings(Order, orders_data)
        session.commit()
        logger.info(f"Successfully loaded {len(orders_data)} orders")
        
    except Exception as e:
        logger.error(f"Error loading orders: {e}")
        session.rollback()
        raise

def load_inventory_bulk(csv_path, session):
    """Load inventory data with bulk operations"""
    try:
        logger.info(f"Loading inventory from {csv_path}")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        df = clean_dataframe(df)
        
        # Prepare bulk insert data
        inventory_data = []
        for _, row in df.iterrows():
            inventory_dict = {
                'id': row.get('id'),
                'product_id': row.get('product_id'),
                'quantity_available': row.get('quantity_available', 0)
            }
            inventory_data.append(inventory_dict)
        
        # Bulk insert with duplicate handling
        try:
            session.bulk_insert_mappings(Inventory, inventory_data)
            session.commit()
            logger.info(f"Successfully loaded {len(inventory_data)} inventory records")
        except IntegrityError as e:
            logger.warning(f"Duplicate inventory records found: {e}")
            session.rollback()
            
            # Handle duplicates
            for inventory_dict in inventory_data:
                try:
                    existing = session.query(Inventory).filter(
                        Inventory.product_id == inventory_dict['product_id']
                    ).first()
                    if existing:
                        # Update existing inventory
                        existing.quantity_available = inventory_dict['quantity_available']
                    else:
                        # Insert new inventory
                        inventory = Inventory(**inventory_dict)
                        session.add(inventory)
                except Exception as e:
                    logger.error(f"Error processing inventory for product {inventory_dict.get('product_id')}: {e}")
                    continue
            
            session.commit()
            logger.info("Inventory loaded with duplicate handling")
            
    except Exception as e:
        logger.error(f"Error loading inventory: {e}")
        session.rollback()
        raise

def create_tables():
    """Create all tables if they don't exist"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise

def main():
    """Main function to load all CSV files"""
    # Define CSV files and their loading functions
    csv_files = [
        ('./data/archive/products.csv', load_products_bulk),
        ('./data/archive/orders.csv', load_orders_bulk),
        ('./data/archive/inventory.csv', load_inventory_bulk)
    ]
    
    # Create tables first
    create_tables()
    
    session = SessionLocal()
    
    try:
        for csv_path, load_function in csv_files:
            if os.path.exists(csv_path):
                load_function(csv_path, session)
            else:
                logger.warning(f"CSV file not found: {csv_path}")
        
        logger.info("Data loading completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        session.rollback()
        sys.exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    main() 