"""
Database Models
SQLite database models for storing product price history
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()


class Product(Base):
    """Product master table - stores unique products"""
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    asin = Column(String(50), unique=True, nullable=False, index=True)
    name = Column(String(500), nullable=False)
    first_seen = Column(DateTime, default=datetime.now)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    is_active = Column(Boolean, default=True)
    
    def __repr__(self):
        return f"<Product(asin='{self.asin}', name='{self.name}')>"


class PriceHistory(Base):
    """Price history table - tracks price changes over time"""
    __tablename__ = 'price_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    asin = Column(String(50), nullable=False, index=True)
    product_name = Column(String(500))
    price = Column(Float)
    rating = Column(Float)
    availability = Column(String(100))
    scraped_at = Column(DateTime, nullable=False, index=True)
    
    def __repr__(self):
        return f"<PriceHistory(asin='{self.asin}', price={self.price}, scraped_at='{self.scraped_at}')>"


class DatabaseManager:
    """Manage database operations"""
    
    def __init__(self, db_path: str = 'data/products.db'):
        """
        Initialize database connection
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.engine = create_engine(f'sqlite:///{db_path}', echo=False)
        self.Session = sessionmaker(bind=self.engine)
        
        # Create tables if they don't exist
        Base.metadata.create_all(self.engine)
        logger.info(f"Database initialized at {db_path}")
    
    def save_products(self, df):
        """
        Save cleaned product data to database
        
        Args:
            df: Pandas DataFrame with cleaned product data
        """
        session = self.Session()
        
        try:
            saved_count = 0
            updated_count = 0
            
            for _, row in df.iterrows():
                # Update or create product
                product = session.query(Product).filter_by(asin=row['asin']).first()
                
                if not product:
                    product = Product(
                        asin=row['asin'],
                        name=row['name'],
                        first_seen=row['scraped_at']
                    )
                    session.add(product)
                    saved_count += 1
                else:
                    product.name = row['name']
                    product.last_updated = datetime.now()
                    updated_count += 1
                
                # Add price history entry
                price_entry = PriceHistory(
                    asin=row['asin'],
                    product_name=row['name'],
                    price=row.get('price'),
                    rating=row.get('rating'),
                    availability=row.get('availability', 'Unknown'),
                    scraped_at=row['scraped_at']
                )
                session.add(price_entry)
            
            session.commit()
            logger.info(f"Saved {saved_count} new products, updated {updated_count} existing products")
            logger.info(f"Added {len(df)} price history entries")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error saving to database: {str(e)}")
            raise
        finally:
            session.close()
    
    def get_product_price_history(self, asin: str):
        """Get price history for a specific product"""
        session = self.Session()
        
        try:
            history = session.query(PriceHistory).filter_by(asin=asin).order_by(
                PriceHistory.scraped_at.desc()
            ).all()
            
            return history
            
        finally:
            session.close()
    
    def get_all_products(self):
        """Get all products"""
        session = self.Session()
        
        try:
            products = session.query(Product).filter_by(is_active=True).all()
            return products
            
        finally:
            session.close()
    
    def get_latest_prices(self):
        """Get latest price for each product"""
        session = self.Session()
        
        try:
            # Get most recent price for each product
            from sqlalchemy import func
            
            subquery = session.query(
                PriceHistory.asin,
                func.max(PriceHistory.scraped_at).label('max_date')
            ).group_by(PriceHistory.asin).subquery()
            
            latest = session.query(PriceHistory).join(
                subquery,
                (PriceHistory.asin == subquery.c.asin) &
                (PriceHistory.scraped_at == subquery.c.max_date)
            ).all()
            
            return latest
            
        finally:
            session.close()
    
    def get_price_changes(self, days: int = 7):
        """
        Get products with price changes in the last N days
        
        Args:
            days: Number of days to look back
        """
        session = self.Session()
        
        try:
            from sqlalchemy import and_
            from datetime import timedelta
            
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Get price history within date range
            recent_prices = session.query(PriceHistory).filter(
                PriceHistory.scraped_at >= cutoff_date
            ).order_by(
                PriceHistory.asin,
                PriceHistory.scraped_at
            ).all()
            
            # Group by ASIN and calculate price changes
            price_changes = {}
            
            for entry in recent_prices:
                if entry.asin not in price_changes:
                    price_changes[entry.asin] = {
                        'name': entry.product_name,
                        'prices': []
                    }
                
                if entry.price:
                    price_changes[entry.asin]['prices'].append({
                        'price': entry.price,
                        'date': entry.scraped_at
                    })
            
            # Calculate changes
            results = []
            for asin, data in price_changes.items():
                if len(data['prices']) >= 2:
                    first_price = data['prices'][0]['price']
                    last_price = data['prices'][-1]['price']
                    change = last_price - first_price
                    change_pct = (change / first_price) * 100 if first_price > 0 else 0
                    
                    results.append({
                        'asin': asin,
                        'name': data['name'],
                        'first_price': first_price,
                        'last_price': last_price,
                        'change': change,
                        'change_pct': change_pct
                    })
            
            return results
            
        finally:
            session.close()


if __name__ == "__main__":
    # Test database operations
    import pandas as pd
    
    # Initialize database
    db = DatabaseManager()
    
    # Test data
    test_data = pd.DataFrame([
        {
            'asin': 'TEST001',
            'name': 'Test Product 1',
            'price': 29.99,
            'rating': 4.5,
            'availability': 'In Stock',
            'scraped_at': datetime.now()
        },
        {
            'asin': 'TEST002',
            'name': 'Test Product 2',
            'price': 49.99,
            'rating': 4.2,
            'availability': 'In Stock',
            'scraped_at': datetime.now()
        }
    ])
    
    # Save data
    db.save_products(test_data)
    
    # Retrieve data
    products = db.get_all_products()
    print(f"\nTotal products: {len(products)}")
    
    latest = db.get_latest_prices()
    print(f"\nLatest prices:")
    for entry in latest:
        print(f"  {entry.asin}: ${entry.price} - {entry.product_name}")
