"""
Data Cleaning and Transformation Module
Cleans and validates scraped product data
"""

import re
import pandas as pd
from datetime import datetime
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Clean and transform scraped product data"""
    
    def clean_products(self, products: List[Dict]) -> pd.DataFrame:
        """
        Clean and standardize product data
        
        Args:
            products: List of raw product dictionaries
            
        Returns:
            Cleaned pandas DataFrame
        """
        logger.info(f"Cleaning {len(products)} products")
        
        # Convert to DataFrame
        df = pd.DataFrame(products)
        
        # Clean each field
        df['name'] = df['name'].apply(self.clean_name)
        df['price'] = df['price'].apply(self.clean_price)
        df['rating'] = df['rating'].apply(self.clean_rating)
        df['availability'] = df['availability'].apply(self.clean_availability)
        
        # Add timestamp if not present
        if 'scraped_at' not in df.columns:
            df['scraped_at'] = datetime.now().isoformat()
        
        # Convert scraped_at to datetime
        df['scraped_at'] = pd.to_datetime(df['scraped_at'])
        
        # Handle missing data
        df = self.handle_missing_data(df)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['asin', 'scraped_at'], keep='last')
        
        logger.info(f"Cleaned data: {len(df)} products remaining")
        
        return df
    
    def clean_name(self, name: str) -> str:
        """Clean product name"""
        if pd.isna(name) or not name:
            return "Unknown Product"
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Limit length
        if len(name) > 200:
            name = name[:197] + "..."
        
        return name
    
    def clean_price(self, price: str) -> float:
        """
        Extract numeric price from string
        
        Examples:
            "$29.99" -> 29.99
            "29,99 €" -> 29.99
            "$1,234.56" -> 1234.56
        """
        if pd.isna(price) or not price:
            return None
        
        try:
            # Remove currency symbols and extra characters
            price_str = str(price)
            
            # Remove common currency symbols
            price_str = re.sub(r'[$€£¥₹]', '', price_str)
            
            # Remove whitespace
            price_str = price_str.strip()
            
            # Handle comma as decimal separator (European format)
            if ',' in price_str and '.' not in price_str:
                price_str = price_str.replace(',', '.')
            else:
                # Remove comma as thousands separator
                price_str = price_str.replace(',', '')
            
            # Extract first number found
            match = re.search(r'\d+\.?\d*', price_str)
            if match:
                return float(match.group())
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not parse price '{price}': {str(e)}")
            return None
    
    def clean_rating(self, rating: str) -> float:
        """
        Extract numeric rating from string
        
        Examples:
            "4.5 out of 5 stars" -> 4.5
            "4.5/5" -> 4.5
            "4.5★" -> 4.5
        """
        if pd.isna(rating) or not rating:
            return None
        
        try:
            rating_str = str(rating)
            
            # Extract first decimal number
            match = re.search(r'\d+\.?\d*', rating_str)
            if match:
                value = float(match.group())
                
                # Ensure rating is between 0 and 5
                if 0 <= value <= 5:
                    return value
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not parse rating '{rating}': {str(e)}")
            return None
    
    def clean_availability(self, availability: str) -> str:
        """Standardize availability status"""
        if pd.isna(availability) or not availability:
            return "Unknown"
        
        avail_lower = str(availability).lower()
        
        if 'in stock' in avail_lower or 'available' in avail_lower:
            return "In Stock"
        elif 'out of stock' in avail_lower or 'unavailable' in avail_lower:
            return "Out of Stock"
        elif 'only' in avail_lower and 'left' in avail_lower:
            # Extract number if present
            match = re.search(r'(\d+)', avail_lower)
            if match:
                return f"Limited Stock ({match.group(1)} left)"
            return "Limited Stock"
        else:
            return availability.strip()
    
    def handle_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset"""
        
        # Log missing data statistics
        missing_counts = df.isnull().sum()
        if missing_counts.sum() > 0:
            logger.info("Missing data summary:")
            for col, count in missing_counts[missing_counts > 0].items():
                logger.info(f"  {col}: {count} missing values")
        
        # Fill missing availability
        df['availability'] = df['availability'].fillna("Unknown")
        
        # Products without price or name are not useful
        df = df.dropna(subset=['name'])
        
        return df
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate cleaned data"""
        
        # Remove rows with invalid prices (negative or too high)
        if 'price' in df.columns:
            df = df[df['price'].isna() | ((df['price'] >= 0) & (df['price'] <= 100000))]
        
        # Remove rows with invalid ratings
        if 'rating' in df.columns:
            df = df[df['rating'].isna() | ((df['rating'] >= 0) & (df['rating'] <= 5))]
        
        logger.info(f"Validation complete: {len(df)} valid products")
        
        return df
    
    def save_cleaned_data(self, df: pd.DataFrame, filename: str = None):
        """Save cleaned data to CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'cleaned_products_{timestamp}.csv'
        
        filepath = f'data/{filename}'
        
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        logger.info(f"Saved cleaned data to {filepath}")
        return filepath


if __name__ == "__main__":
    import json
    
    # Test with sample data
    sample_data = [
        {
            'asin': 'TEST001',
            'name': '  Wireless   Headphones  ',
            'price': '$29.99',
            'rating': '4.5 out of 5 stars',
            'availability': 'In Stock',
            'scraped_at': datetime.now().isoformat()
        },
        {
            'asin': 'TEST002',
            'name': 'USB Cable',
            'price': '15,99 €',
            'rating': '4.2/5',
            'availability': 'Only 3 left in stock',
            'scraped_at': datetime.now().isoformat()
        }
    ]
    
    cleaner = DataCleaner()
    cleaned_df = cleaner.clean_products(sample_data)
    cleaned_df = cleaner.validate_data(cleaned_df)
    
    print("\nCleaned Data:")
    print(cleaned_df.to_string())
    
    filepath = cleaner.save_cleaned_data(cleaned_df)
    print(f"\nSaved to: {filepath}")
