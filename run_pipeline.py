"""
Run Product Price Tracker Pipeline
Simple runner for the complete pipeline - uses Amazon Egypt as example
"""

import sys
import os
from datetime import datetime
import io

# Fix Windows console encoding for emojis
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapers.product_scraper import ProductScraper
from transformers.data_cleaner import DataCleaner
from storage.database import DatabaseManager


def run_pipeline(region='egypt'):
    """Execute the complete data pipeline"""
    
    print("="*70)
    print(f"PRODUCT PRICE TRACKER PIPELINE - Books ToScrape")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    try:
        # Step 1: Scrape Products
        print("\n" + "="*70)
        print(f"STEP 1: SCRAPING PRODUCTS - Books ToScrape")
        print("="*70)
        
        scraper = ProductScraper(region=region)
        
        # Scrape real data from books.toscrape.com (a site that allows scraping!)
        # This site is specifically designed for learning web scraping
        
        products = scraper.scrape_books(max_pages=1)  # Scrape 1 page (~20 books)
        
        print(f"✓ Successfully scraped {len(products)} products from Books ToScrape")        # Save raw data
        raw_filepath = scraper.save_raw_data(products)
        print(f"✓ Raw data saved to: {raw_filepath}")
        
        # Show sample
        if products:
            print(f"\n📦 Sample Product:")
            sample = products[0]
            print(f"   Name: {sample.get('name_en', sample['name'])}")
            if 'name_ar' in sample and region == 'egypt':
                print(f"   Arabic: {sample['name_ar']}")
            print(f"   Price: {sample['price']}")
            print(f"   Rating: {sample['rating']} ⭐")
        
        
        # Step 2: Clean Data
        print("\n" + "="*70)
        print("STEP 2: CLEANING DATA")
        print("="*70)
        
        cleaner = DataCleaner()
        
        # Prepare data for cleaning
        products_for_cleaning = []
        for p in products:
            cleaned_product = {
                'asin': p.get('asin', 'N/A'),
                'name': p.get('name_en', p.get('name', 'Unknown')),
                'price': p.get('price', ''),
                'rating': p.get('rating', ''),
                'availability': p.get('availability', 'Unknown'),
                'scraped_at': p.get('scraped_at', datetime.now().isoformat())
            }
            products_for_cleaning.append(cleaned_product)
        
        cleaned_df = cleaner.clean_products(products_for_cleaning)
        cleaned_df = cleaner.validate_data(cleaned_df)
        
        print(f"✓ Successfully cleaned {len(cleaned_df)} products")
        
        # Save cleaned data
        clean_filepath = cleaner.save_cleaned_data(cleaned_df)
        print(f"✓ Cleaned data saved to: {clean_filepath}")
        
        # Show price statistics
        if len(cleaned_df) > 0 and 'price' in cleaned_df.columns:
            prices = cleaned_df['price'].dropna()
            if len(prices) > 0:
                currency = products[0].get('currency', 'EGP')
                print(f"\n💰 Price Statistics ({currency}):")
                print(f"   Average: {prices.mean():.2f} {currency}")
                print(f"   Minimum: {prices.min():.2f} {currency}")
                print(f"   Maximum: {prices.max():.2f} {currency}")
        
        
        # Step 3: Save to Database
        print("\n" + "="*70)
        print("STEP 3: SAVING TO DATABASE")
        print("="*70)
        
        db = DatabaseManager()
        db.save_products(cleaned_df)
        
        print(f"✓ Successfully saved {len(cleaned_df)} products to database")
        
        
        # Step 4: Generate Report
        print("\n" + "="*70)
        print("STEP 4: GENERATING REPORT")
        print("="*70)
        
        # Get all products
        all_products = db.get_all_products()
        print(f"\nTotal products in database: {len(all_products)}")
        
        # Get latest prices
        latest_prices = db.get_latest_prices()
        
        if latest_prices:
            print(f"\n Latest Prices (Top 10):")
            print("-" * 70)
            
            for i, entry in enumerate(latest_prices[:10], 1):
                price_str = f"{entry.price:.2f}" if entry.price else "N/A"
                rating_str = f"{entry.rating:.1f}" if entry.rating else "N/A"
                
                # Truncate name for display
                name = entry.product_name[:45] + "..." if len(entry.product_name) > 45 else entry.product_name
                
                print(f"{i:2}. {name:48} | {price_str:10} | {rating_str}")
        
        # Check for price changes
        price_changes = db.get_price_changes(days=7)
        
        if price_changes:
            print(f"\n Price Changes (Last 7 Days):")
            print("-" * 70)
            
            for item in sorted(price_changes, key=lambda x: abs(x['change_pct']), reverse=True)[:5]:
                direction = " UP  " if item['change'] > 0 else "📉 DOWN"
                name = item['name'][:40] + "..." if len(item['name']) > 40 else item['name']
                currency = products[0].get('currency', 'EGP')
                print(f"{direction} | {name}")
                print(f"         {item['first_price']:.2f} {currency} → {item['last_price']:.2f} {currency} "
                      f"({item['change_pct']:+.1f}%)")
        else:
            print("\n No price changes detected yet")
            print("   (Run the pipeline multiple times to track changes)")
        
        
        # Success summary
        print("\n" + "="*70)
        print("✓ PIPELINE COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nDatabase location: data/products.db")
        print(f"Raw data: {raw_filepath}")
        print(f"Cleaned data: {clean_filepath}")
        print(f"\n Source: Books ToScrape (scraping-friendly practice site)")
        print(f"   Currency: {products[0].get('currency', 'N/A')}")
        
        return True
        
    except Exception as e:
        print("\n" + "="*70)
        print("✗ PIPELINE FAILED")
        print("="*70)
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Run with Amazon Egypt by default (can change to 'us', 'uk', 'uae', etc.)
    success = run_pipeline(region='egypt')
    sys.exit(0 if success else 1)
