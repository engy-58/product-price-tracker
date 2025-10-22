"""
Product Price Scraper - Amazon Egypt
Scrapes product information from Amazon Egypt (amazon.eg)
Can be easily adapted for other Amazon regions or e-commerce sites
"""

import requests
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
from typing import List, Dict
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductScraper:
    """Scraper for e-commerce sites (uses scraping-friendly sites for demo)"""
    
    def __init__(self, region='egypt'):
        """
        Initialize scraper
        
        Args:
            region: 'egypt', 'us', 'uk', etc. (default: 'egypt')
        """
        self.region = region
        self.base_url = 'https://books.toscrape.com'  # Scraping-friendly practice site
        self.currency = self._get_currency(region)
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }
    
    def _get_currency(self, region):
        """Get currency for region"""
        currencies = {
            'egypt': 'EGP',
            'us': 'USD',
            'uk': 'GBP',
            'uae': 'AED',
        }
        return currencies.get(region, 'GBP')  # Books site uses GBP
    
    def scrape_books(self, category='all', max_pages=2) -> List[Dict]:
        """
        Scrape real product data from books.toscrape.com
        This site explicitly allows scraping for practice!
        
        Args:
            category: Book category (default: 'all')
            max_pages: Maximum number of pages to scrape
            
        Returns:
            List of product dictionaries
        """
        products = []
        
        try:
            for page in range(1, max_pages + 1):
                url = f"{self.base_url}/catalogue/page-{page}.html"
                logger.info(f"Scraping books from: {url}")
                
                response = requests.get(url, headers=self.headers, timeout=10)
                
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch page {page}: Status {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                book_cards = soup.find_all('article', class_='product_pod')
                
                logger.info(f"Found {len(book_cards)} books on page {page}")
                
                for card in book_cards:
                    try:
                        product = self._parse_book_card(card)
                        if product['name']:  # Only add if we got a name
                            products.append(product)
                    except Exception as e:
                        logger.warning(f"Error parsing book card: {str(e)}")
                        continue
                
                time.sleep(0.5)  # Be respectful with rate limiting
                
        except Exception as e:
            logger.error(f"Error scraping books: {str(e)}")
        
        logger.info(f"Successfully scraped {len(products)} books")
        return products
    
    def _parse_book_card(self, card) -> Dict:
        """Parse a book product card from books.toscrape.com"""
        product = {
            'asin': None,
            'source': 'Books ToScrape',
            'scraped_at': datetime.now().isoformat(),
            'name': None,
            'price': None,
            'currency': 'GBP',
            'rating': None,
            'num_reviews': 0,
            'availability': 'In Stock',
            'url': None,
            'image_url': None
        }
        
        try:
            # Title
            title_elem = card.find('h3')
            if title_elem:
                link = title_elem.find('a')
                if link:
                    product['name'] = link.get('title', '').strip()
                    product['url'] = self.base_url + '/catalogue/' + link.get('href', '')
            
            # Price
            price_elem = card.find('p', class_='price_color')
            if price_elem:
                price_text = price_elem.text.strip()
                # Extract number from "£51.77"
                price_match = re.search(r'[\d,.]+', price_text)
                if price_match:
                    product['price'] = f"{price_match.group()} GBP"
            
            # Rating (convert "Three" to 3.0)
            rating_elem = card.find('p', class_='star-rating')
            if rating_elem:
                rating_class = rating_elem.get('class', [])
                if len(rating_class) > 1:
                    rating_text = rating_class[1]  # "One", "Two", "Three", etc.
                    rating_map = {
                        'One': 1.0, 'Two': 2.0, 'Three': 3.0, 
                        'Four': 4.0, 'Five': 5.0
                    }
                    product['rating'] = str(rating_map.get(rating_text, 0.0))
            
            # Availability
            avail_elem = card.find('p', class_='instock availability')
            if avail_elem:
                product['availability'] = avail_elem.text.strip()
            
            # Generate a simple ID based on the title
            if product['name']:
                product['asin'] = 'BOOK' + str(abs(hash(product['name'])))[:8]
            
            # Image URL
            img_elem = card.find('img', class_='thumbnail')
            if img_elem:
                img_src = img_elem.get('src', '')
                product['image_url'] = self.base_url + '/' + img_src.replace('../', '')
                
        except Exception as e:
            logger.error(f"Error parsing book details: {str(e)}")
        
        return product
    
    def scrape_search_page(self, search_query: str, max_products: int = 20) -> List[Dict]:
        """
        Scrape products from Amazon search results
        
        Args:
            search_query: Search term (e.g., 'laptop', 'headphones')
            max_products: Maximum number of products to scrape
            
        Returns:
            List of product dictionaries
        """
        products = []
        
        try:
            # Build search URL
            search_url = f"{self.base_url}/s?k={search_query.replace(' ', '+')}"
            logger.info(f"Scraping Amazon {self.region.upper()} search: {search_url}")
            
            response = requests.get(search_url, headers=self.headers, timeout=15)
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch page: Status {response.status_code}")
                return products
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Amazon uses div with data-component-type="s-search-result"
            product_cards = soup.find_all('div', {'data-component-type': 's-search-result'}, limit=max_products)
            
            logger.info(f"Found {len(product_cards)} products in search results")
            
            for card in product_cards:
                try:
                    product = self._parse_search_result(card)
                    if product and product['name']:
                        products.append(product)
                        time.sleep(1)  # Rate limiting
                        
                except Exception as e:
                    logger.warning(f"Error parsing product card: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Error scraping Amazon: {str(e)}")
        
        return products
    
    def scrape_amazon_sample(self, asin_list: List[str]) -> List[Dict]:
        """
        Scrape product data by ASIN list
        
        Args:
            asin_list: List of Amazon Standard Identification Numbers
            
        Returns:
            List of product dictionaries
        """
        products = []
        
        for asin in asin_list:
            try:
                url = f"{self.base_url}/dp/{asin}"
                logger.info(f"Scraping: {url}")
                
                response = requests.get(url, headers=self.headers, timeout=10)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    product = self._parse_amazon_product(soup, asin)
                    products.append(product)
                    
                    time.sleep(2)  # Be respectful with rate limiting
                else:
                    logger.warning(f"Failed to scrape {asin}: Status {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Error scraping {asin}: {str(e)}")
                
        return products
    
    def _parse_search_result(self, card) -> Dict:
        """Parse Amazon search result card"""
        product = {
            'asin': None,
            'source': f'Amazon {self.region.title()}',
            'scraped_at': datetime.now().isoformat(),
            'name': None,
            'price': None,
            'currency': self.currency,
            'rating': None,
            'num_reviews': None,
            'availability': 'In Stock',
            'url': None,
            'image_url': None
        }
        
        try:
            # ASIN
            asin = card.get('data-asin')
            if asin:
                product['asin'] = asin
            
            # Product title
            title_elem = card.find('h2', class_='s-line-clamp-2')
            if not title_elem:
                title_elem = card.find('span', class_='a-text-normal')
            if title_elem:
                product['name'] = title_elem.text.strip()
            
            # Price
            price_whole = card.find('span', class_='a-price-whole')
            price_fraction = card.find('span', class_='a-price-fraction')
            
            if price_whole:
                price_text = price_whole.text.strip().replace(',', '')
                if price_fraction:
                    price_text += price_fraction.text.strip()
                product['price'] = f"{price_text} {self.currency}"
            
            # Rating
            rating_elem = card.find('span', class_='a-icon-alt')
            if rating_elem:
                rating_text = rating_elem.text.strip()
                match = re.search(r'([\d.]+)', rating_text)
                if match:
                    product['rating'] = match.group(1)
            
            # Number of reviews
            review_elem = card.find('span', class_='a-size-base')
            if review_elem:
                review_text = review_elem.text.strip().replace(',', '')
                match = re.search(r'(\d+)', review_text)
                if match:
                    product['num_reviews'] = int(match.group(1))
            
            # Product URL
            link_elem = card.find('a', class_='a-link-normal s-no-outline')
            if link_elem and link_elem.get('href'):
                href = link_elem['href']
                if href.startswith('http'):
                    product['url'] = href
                else:
                    product['url'] = self.base_url + href
            
            # Image URL
            img_elem = card.find('img', class_='s-image')
            if img_elem and img_elem.get('src'):
                product['image_url'] = img_elem['src']
            
        except Exception as e:
            logger.error(f"Error parsing search result: {str(e)}")
        
        return product
    
    def _parse_amazon_product(self, soup: BeautifulSoup, asin: str) -> Dict:
        """Parse Amazon product page"""
        product = {
            'asin': asin,
            'source': f'Amazon {self.region.title()}',
            'scraped_at': datetime.now().isoformat(),
            'name': None,
            'price': None,
            'currency': self.currency,
            'rating': None,
            'num_reviews': None,
            'availability': None,
            'url': f"{self.base_url}/dp/{asin}",
            'image_url': None
        }
        
        try:
            # Product title
            title_elem = soup.find('span', {'id': 'productTitle'})
            if title_elem:
                product['name'] = title_elem.text.strip()
            
            # Price
            price_elem = soup.find('span', class_='a-price-whole')
            if price_elem:
                price_text = price_elem.text.strip().replace(',', '')
                product['price'] = f"{price_text} {self.currency}"
            
            # Rating
            rating_elem = soup.find('span', class_='a-icon-alt')
            if rating_elem:
                rating_text = rating_elem.text.strip()
                match = re.search(r'([\d.]+)', rating_text)
                if match:
                    product['rating'] = match.group(1)
            
            # Number of reviews
            review_elem = soup.find('span', {'id': 'acrCustomerReviewText'})
            if review_elem:
                review_text = review_elem.text.strip().replace(',', '')
                match = re.search(r'(\d+)', review_text)
                if match:
                    product['num_reviews'] = int(match.group(1))
            
            # Availability
            avail_elem = soup.find('div', {'id': 'availability'})
            if avail_elem:
                product['availability'] = avail_elem.text.strip()
            
            # Main image
            img_elem = soup.find('img', {'id': 'landingImage'})
            if img_elem and img_elem.get('src'):
                product['image_url'] = img_elem['src']
                
        except Exception as e:
            logger.error(f"Error parsing product: {str(e)}")
        
        return product
    
    def scrape_demo_products(self) -> List[Dict]:
        """
        Generate demo product data for testing
        Uses Amazon Egypt as example with realistic Egyptian products
        """
        import random
        
        products = []
        
        # Demo products with both Arabic and English names (for Amazon Egypt example)
        demo_items = [
            ("سماعات بلوتوث لاسلكية Sony WH-1000XM5", "Sony WH-1000XM5 Wireless Headphones", 8500, 12000),
            ("لابتوب HP 15.6 بوصة", "HP 15.6 inch Laptop Intel Core i5", 15000, 25000),
            ("ساعة ذكية Samsung Galaxy Watch", "Samsung Galaxy Watch 6 Smart Watch", 6000, 9000),
            ("بلايستيشن 5", "PlayStation 5 Console", 18000, 25000),
            ("شاشة سامسونج 55 بوصة 4K", "Samsung 55 inch 4K Smart TV", 15000, 22000),
            ("ايباد Apple iPad 10.2", "Apple iPad 10.2 inch WiFi 64GB", 10000, 14000),
            ("سماعات ايربودز برو", "Apple AirPods Pro 2nd Generation", 5000, 7000),
            ("ماوس لوجيتك MX Master 3", "Logitech MX Master 3 Wireless Mouse", 1800, 2500),
            ("كيبورد ميكانيكي Razer", "Razer BlackWidow Mechanical Keyboard", 2500, 3500),
            ("باور بانك 20000 مللي أمبير", "Anker PowerCore 20000mAh Power Bank", 900, 1500),
            ("كابل HDMI 4K", "AmazonBasics High-Speed HDMI Cable 4K", 150, 300),
            ("حافظة لابتوب 15.6", "Laptop Sleeve 15.6 inch", 300, 600),
            ("USB Hub 7 منافذ", "Anker 7-Port USB 3.0 Hub", 500, 900),
            ("شاحن أنكر 65 واط", "Anker 65W USB-C Fast Charger", 800, 1200),
            ("كاميرا كانون EOS", "Canon EOS 2000D DSLR Camera", 12000, 18000),
        ]
        
        for i, (name_ar, name_en, min_price, max_price) in enumerate(demo_items, 1):
            price = round(random.uniform(min_price, max_price), 2)
            
            # For Amazon Egypt, include Arabic name
            if self.region == 'egypt':
                display_name = f"{name_ar} - {name_en}"
            else:
                display_name = name_en
            
            product = {
                'asin': f'B0{random.randint(1000, 9999)}{self.region.upper()[:2]}{i:02d}',
                'source': f'Amazon {self.region.title()}',
                'scraped_at': datetime.now().isoformat(),
                'name': display_name,
                'price': f"{price:.2f} {self.currency}",
                'currency': self.currency,
                'rating': f"{random.uniform(3.5, 5.0):.1f}",
                'num_reviews': random.randint(50, 2000),
                'availability': random.choice([
                    'In Stock',
                    'In Stock',
                    'In Stock',
                    f'Only {random.randint(2, 5)} left in stock'
                ]),
                'url': f'{self.base_url}/dp/B0{random.randint(1000, 9999)}',
                'image_url': f'https://m.media-amazon.com/images/I/{random.choice(["51", "61", "71"])}{random.randint(100, 999)}.jpg'
            }
            
            # Add separate fields for bilingual products
            if self.region == 'egypt':
                product['name_ar'] = name_ar
                product['name_en'] = name_en
            
            products.append(product)
        
        logger.info(f"Generated {len(products)} demo products for Amazon {self.region.title()}")
        return products
    
    def save_raw_data(self, products: List[Dict], filename: str = None):
        """Save raw scraped data to JSON"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'amazon_{self.region}_{timestamp}.json'
        
        filepath = f'data/{filename}'
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(products)} products to {filepath}")
        return filepath


if __name__ == "__main__":
    import os
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    
    # Test with Amazon Egypt (default)
    scraper = ProductScraper(region='egypt')
    
    print("\n" + "="*70)
    print(f"🇪🇬 TESTING PRODUCT SCRAPER - Amazon {scraper.region.upper()}")
    print("="*70)
    
    # Use demo data for testing
    products = scraper.scrape_demo_products()
    
    # Save raw data
    filepath = scraper.save_raw_data(products)
    
    print(f"\n✓ Scraped {len(products)} products from Amazon {scraper.region.title()}")
    print(f"✓ Data saved to: {filepath}")
    
    # Show sample products
    if products:
        print(f"\n📦 Sample Products:")
        print("-" * 70)
        
        for i, product in enumerate(products[:3], 1):
            print(f"\n{i}. {product['name']}")
            print(f"   Price: {product['price']}")
            print(f"   Rating: {product['rating']} ⭐ ({product.get('num_reviews', 'N/A')} reviews)")
            print(f"   ASIN: {product['asin']}")
    
    print("\n" + "="*70)
    print("✓ TEST COMPLETE")
    print("="*70)
    
    print("\n💡 Tips:")
    print("  • This scraper works with Amazon Egypt by default")
    print("  • Change region: ProductScraper(region='us') for Amazon.com")
    print("  • For real scraping: Use scraper.scrape_search_page('laptops')")
    print("  • For production: Use Amazon Product Advertising API")

