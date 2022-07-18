from bs4 import BeautifulSoup
import requests
import pandas as pd
import unicodedata


# Connect to site
text_html = requests.get('https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie').text

# Create soup
soup = BeautifulSoup(text_html, 'html.parser')

# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table
post_container = soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')
post_titles = []
post_prices = []

for post in post_container:
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
    post_price = post.find('span', class_ = 'css-rmqm02 eclomwz0').text

    post_price_ns = unicodedata.normalize('NFKD', post_price).replace(' ', '').replace('zł', '')

    post_titles.append(post_title)

    # Inserting None instead of "ask for price" if there's no price mentioned
    if post_price_ns == 'Zapytajocenę':
        post_prices.append(None)
    else:
        post_prices.append(post_price_ns)

print(post_prices)

    

    