"""
Product Price Tracker Pipeline DAG
Airflow DAG for daily book price scraping from books.toscrape.com
Runs daily at 9:00 AM to track price changes over time
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.product_scraper import ProductScraper
from transformers.data_cleaner import DataCleaner
from storage.database import DatabaseManager


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def scrape_products(**context):
    """Task 1: Scrape product prices from books.toscrape.com"""
    print("Starting product scraping from books.toscrape.com...")
    
    scraper = ProductScraper(region='egypt')
    
    # Scrape real books data
    products = scraper.scrape_books(max_pages=2)  # Scrape 2 pages (~40 books)
    
    if not products:
        raise ValueError("No products scraped! Check the website or scraper.")
    
    # Save raw data
    filepath = scraper.save_raw_data(products)
    
    print(f"✓ Scraped {len(products)} books from books.toscrape.com")
    print(f"✓ Raw data saved to: {filepath}")
    
    # Push data to XCom for next task
    context['ti'].xcom_push(key='products', value=products)
    
    return len(products)


def clean_data(**context):
    """Task 2: Clean and validate scraped data"""
    print("Starting data cleaning...")
    
    # Pull data from previous task
    products = context['ti'].xcom_pull(key='products', task_ids='scrape_prices')
    
    if not products:
        raise ValueError("No products received from scraping task")
    
    cleaner = DataCleaner()
    
    # Clean the data
    cleaned_df = cleaner.clean_products(products)
    cleaned_df = cleaner.validate_data(cleaned_df)
    
    # Save cleaned data
    filepath = cleaner.save_cleaned_data(cleaned_df)
    
    print(f"Cleaned {len(cleaned_df)} products")
    print(f"Cleaned data saved to: {filepath}")
    
    # Convert DataFrame to dict for XCom
    cleaned_data = cleaned_df.to_dict('records')
    context['ti'].xcom_push(key='cleaned_products', value=cleaned_data)
    
    return len(cleaned_df)


def save_to_database(**context):
    """Task 3: Save cleaned data to database"""
    print("Saving to database...")
    
    import pandas as pd
    
    # Pull cleaned data from previous task
    cleaned_data = context['ti'].xcom_pull(key='cleaned_products', task_ids='clean_data')
    
    if not cleaned_data:
        raise ValueError("No cleaned data received")
    
    # Convert back to DataFrame
    df = pd.DataFrame(cleaned_data)
    
    # Initialize database and save
    db = DatabaseManager()
    db.save_products(df)
    
    print(f"Saved {len(df)} products to database")
    
    return len(df)


def generate_report(**context):
    """Task 4: Generate price change report"""
    print("Generating price change report...")
    
    db = DatabaseManager()
    
    # Get price changes in last 7 days
    changes = db.get_price_changes(days=7)
    
    if changes:
        print(f"\n{'='*60}")
        print("PRICE CHANGE REPORT - Last 7 Days")
        print(f"{'='*60}\n")
        
        for item in sorted(changes, key=lambda x: abs(x['change_pct']), reverse=True)[:10]:
            direction = "📈" if item['change'] > 0 else "📉"
            print(f"{direction} {item['name'][:50]}")
            print(f"   ASIN: {item['asin']}")
            print(f"   Price: £{item['first_price']:.2f} → £{item['last_price']:.2f}")
            print(f"   Change: £{item['change']:+.2f} ({item['change_pct']:+.1f}%)")
            print()
    else:
        print("No significant price changes detected")
    
    # Get latest prices
    latest = db.get_latest_prices()
    print(f"\nTotal products tracked: {len(latest)}")
    
    return len(changes) if changes else 0


# Define the DAG
dag = DAG(
    'product_price_tracker',
    default_args=default_args,
    description='Daily product price tracking pipeline',
    schedule_interval='0 9 * * *',  # Run daily at 9 AM
    catchup=False,
    tags=['scraping', 'prices', 'ecommerce'],
)


# Define tasks
task_scrape = PythonOperator(
    task_id='scrape_prices',
    python_callable=scrape_products,
    dag=dag,
)

task_clean = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

task_save = PythonOperator(
    task_id='save_to_db',
    python_callable=save_to_database,
    dag=dag,
)

task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)


# Set task dependencies: scrape → clean → save → report
task_scrape >> task_clean >> task_save >> task_report
