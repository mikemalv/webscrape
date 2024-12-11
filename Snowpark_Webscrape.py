# Configuration Variables
BASE_URL    = "https://www.2ndswing.com"
WEBSITE_URL = "https://www.2ndswing.com/golf-clubs/iron-sets"  # Change this URL as needed
MAX_PAGES   = 15  # Set to None for all pages, or a number for limited pages
SLEEP_TIME  = 2  # Seconds to wait between requests
SAVE_TO_CSV = True  # Whether to save results to CSV file

import requests
from bs4 import BeautifulSoup
import time
from typing import Dict, List
import logging
import pandas as pd
from datetime import datetime
import hashlib
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, current_timestamp
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"golf_scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

class GolfClubScraper:
    def __init__(self, base_url):
        # Get account credentials from a json file
        with open("snowpark_con.json") as f:
            data = json.load(f)
            username = data["username"]
            password = data["password"]
            account = data["account"]

        # Specify connection parameters
        self.connection_parameters = {
            "account": account,
            "user": username,
            "password": password,
            "role": "accountadmin",
            "warehouse": "COMPUTE_WH",
            "database": "CALLAWAY",
            "schema": "PUBLIC",
        }
        
        self.base_url = BASE_URL
        self.scrape_url = base_url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.snowpark_session = None
        print(f"Initializing golf club scraper for: {base_url}")

    def _make_request(self, url: str, retry_count: int = 3) -> requests.Response:
        for attempt in range(retry_count):
            try:
                response = self.session.get(url)
                response.raise_for_status()
                print(f"Successfully loaded page: {url}")
                time.sleep(SLEEP_TIME)
                return response
            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}/{retry_count} failed: {str(e)}")
                if attempt == retry_count - 1:
                    raise e
                time.sleep(2 ** attempt)
        return None

    def _clean_price(self, price_str: str) -> float:
        try:
            return float(price_str.replace('$', '').replace(',', '').strip())
        except:
            return None

    def _create_driver_id(self, row):
        unique_string = f"{row['Brand']}_{row['Model']}_{row['Condition']}_{row['Dexterity']}_{row['Loft']}_{row['Flex']}_{row['Shaft']}"
        return hashlib.md5(unique_string.lower().encode()).hexdigest()

    def parse_product(self, product_element) -> Dict:
        try:
            product_info = {
                'Brand': '',
                'Model': '',
                'Price': None,
                'Condition': '',
                'Dexterity': '',
                'Loft': '',
                'Flex': '',
                'Shaft': '',
                'URL': ''
            }

            # Brand
            brand_elem = product_element.find('div', class_='product-brand')
            if brand_elem:
                product_info['Brand'] = brand_elem.get_text().strip()

            # Model
            model_elem = product_element.find('div', class_='pmp-product-category')
            if model_elem:
                product_info['Model'] = model_elem.get_text().strip()

            # Price
            price_elem = product_element.find('div', class_='current-price')
            if price_elem:
                price_text = price_elem.get_text().strip()
                product_info['Price'] = self._clean_price(price_text)

            # Condition
            condition_elem = product_element.find('div', class_='pmp-product-condition')
            if condition_elem:
                product_info['Condition'] = condition_elem.get_text().strip()

            # Additional attributes
            attributes_elem = product_element.find('div', class_='pmp-product-attributes')
            if attributes_elem:
                attribute_labels = attributes_elem.find_all('span', class_='pmp-attribute-label')
                
                for label in attribute_labels:
                    label_text = label.get_text().strip()
                    value = label.next_sibling
                    if value:
                        value = value.strip(' :,')
                        
                        if 'Dexterity:' in label_text:
                            product_info['Dexterity'] = value
                        elif 'Loft:' in label_text:
                            product_info['Loft'] = value
                        elif 'Flex:' in label_text:
                            product_info['Flex'] = value
                        elif 'Shaft:' in label_text:
                            product_info['Shaft'] = value

            # URL
            url_element = product_element.find('a', class_='product-item-link')
            if url_element:
                product_info['URL'] = self.base_url + url_element.get('href', '')

            return product_info

        except Exception as e:
            print(f"Error parsing product: {str(e)}")
            return None

    def scrape_products(self) -> pd.DataFrame:
        products = []
        current_page = 1

        print(f"\nStarting to scrape products... (Max pages: {MAX_PAGES if MAX_PAGES else 'All'})")
        
        while True:
            try:
                if MAX_PAGES and current_page > MAX_PAGES:
                    print(f"\nReached maximum pages limit ({MAX_PAGES})")
                    break

                url = self.scrape_url
                if current_page > 1:
                    url = f"{self.scrape_url}?p={current_page}"
                
                print(f"\nScraping page {current_page}")
                response = self._make_request(url)
                
                if not response:
                    break

                soup = BeautifulSoup(response.text, 'html.parser')
                product_elements = soup.find_all('li', class_='item product product-item')

                if not product_elements:
                    print("No products found on this page. Finishing scrape.")
                    break

                print(f"Found {len(product_elements)} products on page {current_page}")

                for element in product_elements:
                    product_info = self.parse_product(element)
                    if product_info:
                        products.append(product_info)

                # Check for next page
                next_page = soup.find('a', class_='next')
                if not next_page:
                    print("Reached last page.")
                    break

                current_page += 1

            except Exception as e:
                print(f"Error on page {current_page}: {str(e)}")
                break

        # Convert to DataFrame
        df = pd.DataFrame(products)
        
        if not df.empty:
            # Add source URL
            df['SOURCE_URL'] = self.scrape_url
            
            # Generate driver IDs
            df['DRIVER_ID'] = df.apply(self._create_driver_id, axis=1)
            
            # Sort by price (descending) and brand
            df = df.sort_values(['Price', 'Brand'], ascending=[False, True])

        print(f"\nScraped {len(df)} products total")
        
        if SAVE_TO_CSV:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f'golf_data_{timestamp}.csv'
            df.to_csv(csv_filename, index=False)
            print(f"\nData saved to {csv_filename}")

        return df

    def prepare_for_snowflake(self, df):
        df_clean = df.copy()
        
        df_clean = df_clean.rename(columns={
            'Brand': 'BRAND',
            'Model': 'MODEL',
            'Price': 'PRICE',
            'Condition': 'CONDITION',
            'Dexterity': 'DEXTERITY',
            'Loft': 'LOFT',
            'Flex': 'FLEX',
            'Shaft': 'SHAFT',
            'URL': 'PRODUCT_URL'
        })
        
        return df_clean

    def upload_to_snowflake(self, df):
        try:
            # Create Snowflake session using instance connection parameters
            self.snowpark_session = Session.builder.configs(self.connection_parameters).create()
            print("Connected to Snowflake successfully")

            # Prepare the data
            df_snow = self.prepare_for_snowflake(df)
            
            # Convert pandas DataFrame to Snowpark DataFrame
            snow_df = self.snowpark_session.create_dataframe(df_snow)
            
            # Create temporary table
            temp_table = "TEMP_GOLF_PRODUCTS"
            snow_df.write.save_as_table(temp_table, mode="overwrite", table_type="temporary")
            
            # Perform upsert operation
            merge_query = f"""
            MERGE INTO GOLF_DRIVERS target
            USING {temp_table} source
            ON target.DRIVER_ID = source.DRIVER_ID
            WHEN MATCHED AND target.PRICE != source.PRICE THEN
                UPDATE SET 
                    PRICE = source.PRICE,
                    SOURCE_URL = source.SOURCE_URL,
                    LAST_UPDATED = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    DRIVER_ID, BRAND, MODEL, PRICE, CONDITION, 
                    DEXTERITY, LOFT, FLEX, SHAFT, PRODUCT_URL,
                    SOURCE_URL, LAST_UPDATED, FIRST_SEEN
                )
                VALUES (
                    source.DRIVER_ID, source.BRAND, source.MODEL, 
                    source.PRICE, source.CONDITION, source.DEXTERITY,
                    source.LOFT, source.FLEX, source.SHAFT,
                    source.PRODUCT_URL, source.SOURCE_URL,
                    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                )
            """
            
            self.snowpark_session.sql(merge_query).collect()
            
            # Get upload statistics
            new_records = self.snowpark_session.sql(f"""
                SELECT COUNT(*) as new_records
                FROM {temp_table} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM GOLF_DRIVERS t
                    WHERE t.DRIVER_ID = s.DRIVER_ID
                )
            """).collect()[0]['NEW_RECORDS']
            
            updated_records = self.snowpark_session.sql(f"""
                SELECT COUNT(*) as updated_records
                FROM {temp_table} s
                JOIN GOLF_DRIVERS t ON t.DRIVER_ID = s.DRIVER_ID
                WHERE t.PRICE != s.PRICE
            """).collect()[0]['UPDATED_RECORDS']
            
            print(f"\nUpload complete:")
            print(f"New records added: {new_records}")
            print(f"Records updated: {updated_records}")
            
        except Exception as e:
            print(f"Error uploading to Snowflake: {str(e)}")
            raise
        
        finally:
            if self.snowpark_session:
                self.snowpark_session.close()

def display_data_summary(df: pd.DataFrame):
    if df.empty:
        print("No data to display")
        return

    print("\n=== Product Inventory Summary ===")
    print(f"Total Products: {len(df)}")
    print(f"Unique Brands: {df['Brand'].nunique()}")
    print(f"Price Range: ${df['Price'].min():.2f} - ${df['Price'].max():.2f}")
    
    print("\n=== Sample of Available Products ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df.head(10))
    
    print("\n=== Brand Distribution ===")
    brand_counts = df['Brand'].value_counts().head()
    print(brand_counts)

if __name__ == "__main__":
    # Initialize scraper
    scraper = GolfClubScraper(WEBSITE_URL)
    
    # Scrape the data
    df = scraper.scrape_products()
    
    # Display summary of scraped data
    display_data_summary(df)
    
    # Upload to Snowflake
    print("\nUploading data to Snowflake...")
    scraper.upload_to_snowflake(df)