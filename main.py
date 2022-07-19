from bs4 import BeautifulSoup
import requests
import pandas as pd
import unicodedata
import csv

# Connecting to csv file with list of polish towns (later we use it to define which string in class is city as there's common class for few elements in one span)
with open('places.csv', encoding='utf-8') as c:
    city = csv.reader(c)
    cities_lists = list(city)
    cities = []
    for city_string in cities_lists:
        for city_string2 in city_string:
            cities.append(city_string2)

# Connect to site
text_html = requests.get('https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie').text

# Create soup
soup = BeautifulSoup(text_html, 'html.parser')

# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table
post_container = soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')
post_titles = []
post_prices = []

for post in post_container:

    # Find & append titles
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
    post_titles.append(post_title)

    # Find & append prices. Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned.
    post_price = post.find('span', class_ = 'css-rmqm02 eclomwz0').text
    post_price_ns = unicodedata.normalize('NFKD', post_price)

    post_price_replace = {' ':'', 'zł': '', ',':'.'}
    for key, value in post_price_replace.items():
        post_price_ns = post_price_ns.replace(key, value)

    if post_price_ns == 'Zapytajocenę':
        post_prices.append(None)
    else:
        post_prices.append(round(int(float(post_price_ns)), 0))

    # find & append cities in common class span
print(post_prices)

    

    