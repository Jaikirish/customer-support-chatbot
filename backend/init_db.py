from sqlalchemy import create_engine
from database import engine, Base
from models import Product, Order, Inventory, OrderStatus
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def init_database():
    """Initialize database by creating all tables"""
    try:
        logger.info("Creating database tables...")
        
        # Create all tables defined in the models
        Base.metadata.create_all(bind=engine)
        
        logger.info("Database tables created successfully!")
        logger.info("Created tables:")
        for table_name in Base.metadata.tables.keys():
            logger.info(f"  - {table_name}")
            
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")
        raise

def drop_all_tables():
    """Drop all tables (use with caution!)"""
    try:
        logger.warning("Dropping all database tables...")
        
        # Drop all tables
        Base.metadata.drop_all(bind=engine)
        
        logger.info("All tables dropped successfully!")
        
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        raise

def check_tables_exist():
    """Check if tables exist in the database"""
    try:
        from sqlalchemy import inspect
        
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        
        logger.info("Existing tables in database:")
        for table in existing_tables:
            logger.info(f"  - {table}")
            
        return existing_tables
        
    except Exception as e:
        logger.error(f"Error checking tables: {e}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "drop":
            drop_all_tables()
        elif command == "check":
            check_tables_exist()
        else:
            print("Usage: python init_db.py [create|drop|check]")
            print("  create - Create all tables (default)")
            print("  drop   - Drop all tables")
            print("  check  - Check existing tables")
    else:
        # Default: create tables
        init_database() 