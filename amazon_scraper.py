import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
from azure.storage.blob import BlobServiceClient

def get_title(soup):
    try:
        title = soup.find('span', attrs={'id': 'productTitle'}).text.strip()
    except AttributeError:
        title = ""
    return title

def get_rating(soup):
    try:
        rating = soup.find('span', attrs={'class': 'a-icon-alt'}).text
    except AttributeError:
        rating = ""
    return rating

def get_price(soup):
    try:
        price = soup.find('span', attrs={'class': 'a-price-whole'}).text.strip('.')
    except AttributeError:
        price = ""
    return price

def get_reviews_count(soup):
    try:
        reviews_count = soup.find('span', attrs={'id': 'acrCustomerReviewText'}).text.strip()
    except AttributeError:
        reviews_count = ""
    return reviews_count

def check_availability(soup):
    try:
        availability = soup.find("div", attrs={'id':'availability'})
        availability = availability.find("span").string.strip()

    except AttributeError:
        availability = "Not Available"	

    return availability

def get_product_links(soup):
    product_links_list = []
    
    product_a_tags = soup.find_all('a', attrs={'a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal'})

    #get all links from a tag 
    for a_tag in product_a_tags:
        href = a_tag.get('href')
        product_link = 'https://www.amazon.in/' + href
        product_links_list.append(product_link)
    
    return product_links_list

def extract_data(product_links_list, HEADER):
    products_data = {
        'title':[],
        'price':[],
        'rating':[],
        'reviews':[],
        'availability':[]
    }

    #loop for extracting data from product links list
    for link in product_links_list:
        product_webpage = requests.get(link, headers=HEADER)
        soup = BeautifulSoup(product_webpage.content, 'html.parser')
        
        #appending data into the products dictionary
        products_data['title'].append(get_title(soup))
        products_data['price'].append(get_price(soup))
        products_data['rating'].append(get_rating(soup))
        products_data['reviews'].append(get_reviews_count(soup))
        products_data['availability'].append(check_availability(soup))

    return products_data

def upload_csv_to_blob(amazon_df):
    
    # Azure Storage connection string and container name
    connection_string = ""
    container_name = ""
    blob_name = "amazon_data.csv"
    
    # Create a blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Create a container client
    container_client = blob_service_client.get_container_client(container_name)

    # Convert DataFrame to CSV string
    csv_data = amazon_df.to_csv(index=False)

    # upload CSV data to Azure Storage container
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(csv_data, overwrite=True)
    
def amazon_data_scraper():
    
    HEADER = ({
    'User-Agent': '',
    'Accept-Language':'en-US, en;q=0.5'
    })

    url = 'https://www.amazon.in/s?k=shirt+for+men&crid=3I7TS8KOXWWV3&sprefix=shirt%2Caps%2C431&ref=nb_sb_ss_ts-doa-p_1_5'
    
    webpage = requests.get(url, headers=HEADER)

    if webpage.status_code == 200:
        soup = BeautifulSoup(webpage.content, 'html.parser')
        
        #extract products links
        product_links_list = get_product_links(soup)
        
        #extract product details
        products_data = extract_data(product_links_list, HEADER)
        
        #converting dictionary into DataFrame
        amazon_df = pd.DataFrame.from_dict(products_data)
        amazon_df['title'].replace('', np.nan, inplace=True)
        amazon_df = amazon_df.dropna(subset=['title'])
        
        #load data to blob storage
        upload_csv_to_blob(amazon_df)
        
        # Return True to indicate successful scraping
        return True, webpage.status_code
    else:
        # Return False if webpage status code is not 200
        return False, webpage.status_code