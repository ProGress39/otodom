from bs4 import BeautifulSoup
import requests
import unicodedata
import csv
import lxml.html
import pyspark
from pyspark.sql import SparkSession
import os
import sys
from joblib import Parallel, delayed
import joblib
import pickle
from pyspark.sql.functions import when, lit


sys.setrecursionlimit(20000)

# Solving problem with PySpark environmental variables
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
    URL_Site = 'https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie&page={}&limit=1500'.format(page)
    req = requests.get(URL_Site).text
    all_pages_html = all_pages_html + req[:-7] #-7 to remove </html> as lxml parser doesn't work properly with it.


#Empty arrays to store data
post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type  = ([] for i in range(6))


#Functions
def append_data(post):
# 1. Find & append titles
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
    post_titles.append(post_title)

# 2. Find & append prices, number of rooms, square metrage (they're in same span class).
    # Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned.     
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
    post_area = post.find('span', class_ = 'css-17o293g es62z2j9').text.split(',')
    for town in post_area:
        if town.strip() in cities_list:
            post_cities.append(town.strip())
            break
        else:
            continue

# 4/5. Find & append square metrage and rooms in common class span.
    post_sq_rooms = post.find_all('span', class_='css-rmqm02 eclomwz0')
    post_rooms.append(int(post_sq_rooms[2].text[0]))
    post_sqmetrage.append(float(post_sq_rooms[3].text.split(' ')[0]))

# 6. Find & append type of post. Can be private or company.
    post_sq_type = post.find('span', class_='css-13vzu28 e1dxhs6v2')
    if post_sq_type != None:
        post_type.append('Private post')
    else:
        post_type.append('Company post')


# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table
main_soup = BeautifulSoup(all_pages_html, 'lxml')
post_container = main_soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')

# Loop to dive into post container and extract informations. Set n_jobs to -1 and it will use all CPU from your device.
if __name__ == '__main__':
    Parallel(n_jobs=1)(delayed(append_data)(post) for post in post_container)

# Create dictionary from lists
posts_dict = [{'Title': post_titles, 'Price': post_prices, 'City': post_cities,
               'SqMetrage': post_sqmetrage, 'Rooms': post_rooms, 'PostType': post_type}
                for post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type
                in zip(post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type)]


spark = SparkSession.builder.getOrCreate()

posts_df = spark.createDataFrame(posts_dict)

modified_posts_df = posts_df.withColumn('SqMetrageBucket', \
    when((posts_df.SqMetrage < 30), lit('<30')) \
        .when((posts_df.SqMetrage >= 30) & (posts_df.SqMetrage <50), lit('30-49')) \
        .when((posts_df.SqMetrage >= 50) & (posts_df.SqMetrage <75), lit('30-74')) \
        .when((posts_df.SqMetrage >= 75) & (posts_df.SqMetrage <= 100), lit('75-100')) \
        .otherwise(lit('>100')) \
        ).withColumn('Urgency', \
    when((posts_df.Title.contains('Pilne')) | (posts_df.Title.contains('Pilnie')), lit('Urgent')) \
    .otherwise(lit('Normal')) \
    ).withColumn('PricePerSqM', (posts_df.Price) / (posts_df.SqMetrage))

pandas_df = modified_posts_df.toPandas()
pandas_df.to_csv(r'C:\Users\mapop\OneDrive\Pulpit\Web Scrapping\Otomoto\posts.csv', index=False, header=True)