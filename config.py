# Configuration settings for Product Price Tracker

# Database settings
DATABASE_PATH = 'data/products.db'

# Scraping settings
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
REQUEST_TIMEOUT = 10
RATE_LIMIT_DELAY = 2  # seconds between requests

# Data settings
MAX_PRODUCT_NAME_LENGTH = 200
PRICE_RANGE = (0, 100000)  # min and max valid price
RATING_RANGE = (0, 5)  # min and max valid rating

# Report settings
PRICE_CHANGE_DAYS = 7  # days to look back for price changes
TOP_PRODUCTS_COUNT = 10  # number of products to show in reports
