from bs4 import BeautifulSoup
import requests
import pandas as pd
import unicodedata
import csv
import lxml.html
from multiprocessing import Pool
import pyspark
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Connecting to otodom list cities to create list of available cities. Based on that we can later exctract city from common class which include other strings.
with open('cities.csv', encoding="utf8") as cities_file:
    reader = csv.reader(cities_file)
    cities = list(reader)
    cities_list = []
    for city_listed in cities:
        for city in city_listed:
            if city != '':
                cities_list.append(city)


# Connect to page
all_pages_html = ''
for page in range(0,2):
    URL_Site = 'https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie&page={}'.format(page)
    req = requests.get(URL_Site).text
    all_pages_html = all_pages_html + req[:-7] #-7 to remove </html> as lxml parser doesn't work properly with it.


#Empty arrays to store data
post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type  = ([] for i in range(6))


#Functions
# 1. Find & append titles
def append_titles():
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
    post_titles.append(post_title)

# 2. Find & append prices, number of rooms, square metrage (they're in same span class).
    # Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned. 
def append_prices():    
    post_price = post.find('span', class_ = 'css-rmqm02 eclomwz0').text
    post_price_ns = unicodedata.normalize('NFKD', post_price)

    post_price_replace = {' ':'', 'zł': '','€':'','$':'', ',':'.'}
    for key, value in post_price_replace.items():
        post_price_ns = post_price_ns.replace(key, value)

    if post_price_ns == 'Zapytajocenę':
        post_prices.append(None)
    else:
        post_prices.append(round(int(float(post_price_ns)), 0))

# 3. Find & append cities in common class span. Comparing with goverment list of polish cities.
def append_cities():
    post_area = post.find('span', class_ = 'css-17o293g es62z2j9').text.split(',')
    for town in post_area:
        if town.strip() in cities_list:
            post_cities.append(town.strip())
            break
        else:
            continue

# 4/5. Find & append square metrage and rooms in common class span.
def append_sqm():
    post_sq_rooms = post.find_all('span', class_='css-rmqm02 eclomwz0')
    post_rooms.append(int(post_sq_rooms[2].text[0]))
    post_sqmetrage.append(float(post_sq_rooms[3].text.split(' ')[0]))

# 6. Find & append type of post. Can be private or company.
def append_type():
    post_sq_type = post.find('span', class_='css-13vzu28 e1dxhs6v2')
    if post_sq_type != None:
        post_type.append('Private post')
    else:
        post_type.append('Company post')


# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table
main_soup = BeautifulSoup(all_pages_html, 'lxml')
post_container = main_soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')

# Loop to dive into post container and extract informations
for post in post_container:
    append_titles()
    append_prices()
    append_cities()
    append_sqm()
    append_type()

# Create dictionary from lists
posts_dict = [{'Title': post_titles, 'Price': post_prices, 'City': post_cities,
               'Sq Metrage': post_sqmetrage, 'Rooms': post_rooms, 'Post type': post_type}
                for post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type
                in zip(post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type)]

spark = SparkSession.builder.getOrCreate()

posts_df = spark.createDataFrame(posts_dict)
type(posts_df)



#przyspieszyć działanie