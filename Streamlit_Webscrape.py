import streamlit as st
import random
from snowflake.snowpark.context import get_active_session
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import hashlib
from datetime import datetime

def make_request(url: str, retry_count: int = 3, base_delay: int = 5) -> requests.Response:
    """
    Make HTTP request with improved retry logic and rate limiting
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }
    session = requests.Session()
    session.headers.update(headers)
    
    for attempt in range(retry_count):
        try:
            # Add randomization to the delay to make requests look more natural
            delay = base_delay + (attempt * 2) + random.uniform(1, 3)
            time.sleep(delay)
            
            response = session.get(url)
            response.raise_for_status()
            
            return response
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # Get retry-after header if it exists, convert to float safely
                retry_after = e.response.headers.get('Retry-After')
                if retry_after:
                    try:
                        retry_after = float(retry_after)
                    except (ValueError, TypeError):
                        retry_after = delay * 2
                else:
                    retry_after = delay * 2
                
                st.warning(f"Rate limited. Waiting {retry_after:.1f} seconds before retrying...")
                time.sleep(retry_after)
                
                if attempt == retry_count - 1:
                    raise
            else:
                raise
                
        except requests.RequestException as e:
            if attempt == retry_count - 1:
                raise
            
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            st.warning(f"Request failed. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{retry_count}")
            time.sleep(wait_time)
    
    return None
    
def clean_price(price_str: str) -> float:
    """Convert price string to float"""
    try:
        return float(price_str.replace('$', '').replace(',', '').strip())
    except:
        return None

def create_driver_id(row):
    """Create a unique hash ID for each driver"""
    unique_string = f"{row['Brand']}_{row['Model']}_{row['Condition']}_{row['Dexterity']}_{row['Loft']}_{row['Flex']}_{row['Shaft']}"
    return hashlib.md5(unique_string.lower().encode()).hexdigest()

def parse_product(product_element):
    """Extract product information from HTML element"""
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
            product_info['Price'] = clean_price(price_text)

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
            product_info['URL'] = "https://www.2ndswing.com" + url_element.get('href', '')

        return product_info

    except Exception as e:
        st.error(f"Error parsing product: {str(e)}")
        return None

def scrape_products(url: str, max_pages: int) -> pd.DataFrame:
    """Scrape products from website"""
    products = []
    current_page = 1
    
    # Create placeholder containers
    progress_bar = st.progress(0)
    progress_container = st.empty()  # For "Scraping page X of Y"
    page_status = st.empty()  # For current URL being processed
    products_status = st.empty()  # Single container for products found message

    while current_page <= max_pages:
        try:
            current_url = url
            if current_page > 1:
                current_url = f"{url}?p={current_page}"
            
            # Update progress
            progress_container.text(f"Scraping page {current_page} of {max_pages}")
            progress_bar.progress(current_page / max_pages)
            page_status.text(f"Processing: {current_url}")
            
            response = make_request(current_url)
            if not response:
                break

            soup = BeautifulSoup(response.text, 'html.parser')
            product_elements = soup.find_all('li', class_='item product product-item')

            if not product_elements:
                page_status.warning("No products found on this page. Finishing scrape.")
                break

            # Update products found in the same container
            products_status.text(f"Found {len(product_elements)} products on page {current_page}")
            
            for element in product_elements:
                product_info = parse_product(element)
                if product_info:
                    products.append(product_info)

            next_page = soup.find('a', class_='next')
            if not next_page:
                page_status.info("Reached last page.")
                break

            current_page += 1

        except Exception as e:
            page_status.error(f"Error on page {current_page}: {str(e)}")
            break

    progress_bar.progress(1.0)
    progress_container.text("Scraping completed!")
    total_products = len(products)
    products_status.text(f"Total products collected: {total_products}")

    df = pd.DataFrame(products)
    if not df.empty:
        df['SOURCE_URL'] = url
        df['DRIVER_ID'] = df.apply(create_driver_id, axis=1)
        df = df.sort_values(['Price', 'Brand'], ascending=[False, True])

    return df

def upload_to_snowflake(df):
    """Upload data to Snowflake"""
    try:
        session = get_active_session()
        st.success("Connected to Snowflake successfully")

        # Prepare the data
        df_snow = df.copy()
        df_snow = df_snow.rename(columns={
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
        
        # Convert to Snowpark DataFrame
        snow_df = session.create_dataframe(df_snow)
        
        # Create a view
        snow_df.create_or_replace_view("GOLF_PRODUCTS_VIEW")
        
        # Perform upsert
        merge_query = """
        MERGE INTO GOLF_DRIVERS target
        USING GOLF_PRODUCTS_VIEW source
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
        
        session.sql(merge_query).collect()
        
        # Get statistics
        new_records = session.sql("""
            SELECT COUNT(*) as new_records
            FROM GOLF_PRODUCTS_VIEW s
            WHERE NOT EXISTS (
                SELECT 1 FROM GOLF_DRIVERS t
                WHERE t.DRIVER_ID = s.DRIVER_ID
            )
        """).collect()[0]['NEW_RECORDS']
        
        updated_records = session.sql("""
            SELECT COUNT(*) as updated_records
            FROM GOLF_PRODUCTS_VIEW s
            JOIN GOLF_DRIVERS t ON t.DRIVER_ID = s.DRIVER_ID
            WHERE t.PRICE != s.PRICE
        """).collect()[0]['UPDATED_RECORDS']
        
        # Clean up
        session.sql("DROP VIEW IF EXISTS GOLF_PRODUCTS_VIEW").collect()
        
        st.success(f"""
        Upload complete:
        - New records added: {new_records}
        - Records updated: {updated_records}
        """)
        
    except Exception as e:
        st.error(f"Error uploading to Snowflake: {str(e)}")
        raise

def main():
    # Add Callaway logo
    col1, col2, col3 = st.columns([1,2,1])
    with col1:
        st.image("https://moongolf.com/wp-content/uploads/2017/03/Callaway-logo-WHITE-1024x591-small-300x173.png", width=150)
    
    st.title("⛳ Golf Club Data Scraper")
    st.write("Enter a URL to scrape golf club data and save it to Snowflake")
    
    # Input form
    with st.form("scraper_form"):
        url = st.text_input("Enter URL to scrape:", "https://www.2ndswing.com/golf-clubs/drivers")
        max_pages = st.number_input("Maximum pages to scrape:", min_value=1, value=10)
        submitted = st.form_submit_button("Start Scraping")
    
    if submitted:
        # Scraping progress
        st.subheader("Scraping Progress")
        df = scrape_products(url, max_pages)
        
        if not df.empty:
            # Display complete results
            st.subheader("Scraped Data")
            st.write(f"Total products found: {len(df)}")
            
            # Add filters
            st.write("Filter the data:")
            col1, col2 = st.columns(2)
            with col1:
                brands = ["All"] + sorted(df['Brand'].unique().tolist())
                selected_brand = st.selectbox("Select Brand", brands)
            
            with col2:
                conditions = ["All"] + sorted(df['Condition'].unique().tolist())
                selected_condition = st.selectbox("Select Condition", conditions)
            
            # Apply filters
            filtered_df = df.copy()
            if selected_brand != "All":
                filtered_df = filtered_df[filtered_df['Brand'] == selected_brand]
            if selected_condition != "All":
                filtered_df = filtered_df[filtered_df['Condition'] == selected_condition]
            
            # Display the filtered dataframe with sorting enabled
            st.dataframe(
                filtered_df,
                use_container_width=True,
                height=400,
                column_config={
                    "Brand": st.column_config.TextColumn("Brand", width=100),
                    "Model": st.column_config.TextColumn("Model", width=200),
                    "Price": st.column_config.NumberColumn("Price", format="$%.2f", width=100),
                    "Condition": st.column_config.TextColumn("Condition", width=100),
                    "Dexterity": st.column_config.TextColumn("Dexterity", width=100),
                    "Loft": st.column_config.TextColumn("Loft", width=80),
                    "Flex": st.column_config.TextColumn("Flex", width=80),
                    "Shaft": st.column_config.TextColumn("Shaft", width=200),
                    "URL": st.column_config.LinkColumn("URL"),
                    "SOURCE_URL": st.column_config.LinkColumn("Source"),
                    "DRIVER_ID": st.column_config.TextColumn("Driver ID", width=200)
                }
            )
            
            # Summary statistics
            st.subheader("Summary Statistics")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Average Price", f"${filtered_df['Price'].mean():.2f}")
            with col2:
                st.metric("Lowest Price", f"${filtered_df['Price'].min():.2f}")
            with col3:
                st.metric("Highest Price", f"${filtered_df['Price'].max():.2f}")
            
            # Upload to Snowflake
            st.subheader("Uploading to Snowflake")
            upload_to_snowflake(df)
            
            # Download option
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name=f'golf_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv',
                mime='text/csv',
            )
        else:
            st.error("No data was scraped. Please check the URL and try again.")

if __name__ == "__main__":
    main()