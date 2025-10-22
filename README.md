# Product Price Tracker 📊

A Python web scraping project that tracks product prices from **books.toscrape.com** over time. This site explicitly allows scraping for learning purposes - perfect for demonstrating real web scraping skills!

## What Does It Do?

This project scrapes **real product information** from books.toscrape.com (a scraping-friendly practice website), cleans the data, stores it in a database, and shows you how prices change over time. It demonstrates a complete data pipeline with actual web scraping!

## Features

- **Real Web Scraping**: Actually scrapes live data from books.toscrape.com
- **Data Cleaning**: Organizes messy HTML data into clean structured format
- **Price Tracking**: Saves prices to a database so you can see trends
- **Reports**: Shows you which products went up or down in price
- **Legal & Ethical**: Uses a site specifically designed for scraping practice

## How to Run It 🚀

### Option 1: Simple Runner (Recommended for Testing)

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline once
python run_pipeline.py
```

### Option 2: Automated Scheduling with Airflow

For automated daily runs:

```bash
# Install dependencies (includes Airflow)
pip install -r requirements.txt

# Initialize Airflow database
airflow db init

# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start Airflow webserver (in one terminal)
airflow webserver --port 8080

# Start Airflow scheduler (in another terminal)
airflow scheduler
```

Then:

1. Open http://localhost:8080 in your browser
2. Login with your admin credentials
3. Enable the `product_price_tracker` DAG
4. The pipeline will run daily at 9:00 AM automatically!

### What the Pipeline Does:

1. **Scrape real products** from books.toscrape.com (20 books per run)
2. Clean and organize the data
3. Save everything to a database
4. Show you a nice report with price changes

> **Note**: Uses books.toscrape.com, a website specifically created for practicing web scraping legally!

## What I Learned

- **Real web scraping** using BeautifulSoup and requests
- Working with **pandas** for data cleaning
- Using **SQLite** databases with SQLAlchemy
- Building **ETL data pipelines** (Extract → Transform → Load)
- Handling **HTTP requests** and parsing HTML
- **Legal and ethical** web scraping practices

## Project Structure 📁

```
product-price-tracker/
├── scrapers/           # Web scraping code
├── transformers/       # Data cleaning
├── storage/            # Database stuff
├── dags/              # Airflow scheduling (optional)
├── data/              # Where all the data goes
└── run_pipeline.py    # Main script to run everything
```

## Example Output

```
✓ Successfully scraped 20 products from Books ToScrape
✓ Cleaned data saved to: data/cleaned_products.csv
✓ Successfully saved 20 products to database

📊 Latest Prices (Top 10):
1. A Light in the Attic                    | 51.77 GBP | ⭐3.0
2. Tipping the Velvet                      | 53.74 GBP | ⭐1.0
3. Soumission                              | 50.10 GBP | ⭐1.0

💰 Price Changes (Last 7 Days):
📈 UP   | A Light in the Attic: 48.50 GBP → 51.77 GBP (+6.7%)
📉 DOWN | Tipping the Velvet: 55.00 GBP → 53.74 GBP (-2.3%)
```

## Technologies Used 🛠️

- **Python 3.12** - Main programming language
- **BeautifulSoup4** - For scraping web pages
- **Pandas** - For cleaning and organizing data
- **SQLAlchemy** - For database operations
- **SQLite** - Simple database to store products
- **Apache Airflow 2.10** - For automating daily runs and workflow orchestration

## Notes 📝

- ✅ **Scrapes real data** from books.toscrape.com (a site designed for scraping practice)
- ✅ **Legal and ethical** - the website explicitly allows scraping
- ✅ Demonstrates complete web scraping workflow
- ✅ **Two execution modes**: Simple runner OR Airflow automation
- ✅ Database grows over time to show historical price trends
- 🔄 **Airflow DAG** runs daily at 9:00 AM (configurable)
- ✅ Can be adapted for other scraping-friendly sites
- Perfect for portfolios and learning projects!

## Future Improvements 💡

- Add more scraping-friendly sites (quotes.toscrape.com, scrapethissite.com)
- Scrape multiple pages for more products
- Email notifications when prices drop
- Web dashboard to visualize price trends
- Export reports to PDF
- Add product categories and filtering
- Price alert system for specific products
