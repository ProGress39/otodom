from bs4 import BeautifulSoup
import requests
import unicodedata
import csv
import lxml.html
import pyspark
from pyspark.sql import SparkSession, DataFrameWriter
import os
import sys
from joblib import Parallel, delayed
import joblib
import pickle
from pyspark.sql.functions import when, lit, current_date

sys.setrecursionlimit(20000)

# Solving problem with PySpark environmental variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


#Setting environmental variables for db credentials
mysql_username = os.environ.get('MYSQL_USERNAME')
mysql_password = os.environ.get('MYSQL_PASSWORD')


# Connecting to otodom list cities to create list of available cities. Based on that we can later exctract city from common class which include other strings.
with open('cities.csv', encoding="utf8") as cities_file:
    reader = csv.reader(cities_file)
    cities = list(reader)
    cities_list = []
    for city_listed in cities:
        for city in city_listed:
            if city != '':
                cities_list.append(city)


# Connect to otodom webpage and append htmls to strings. Seperate variables for sales, rents and room rents
flat_sale_htmls, flat_rent_htmls, room_rent_htmls = ('' for i in range(3))

for page in range(0,2):
    sale_url = 'https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie&page={}&limit=1000'.format(page)
    req = requests.get(sale_url).text
    flat_sale_htmls = flat_sale_htmls + req[:-7] #-7 to remove </html> as lxml parser doesn't work properly with it.

for page in range(0,2):
    rent_url = 'https://www.otodom.pl/pl/oferty/wynajem/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=wynajem&searchingCriteria=mieszkanie&page={}&limit=1000'.format(page)
    req = requests.get(rent_url).text
    flat_rent_htmls = flat_rent_htmls + req[:-7]

for page in range(0,2):
    room_url = 'https://www.otodom.pl/pl/oferty/wynajem/pokoj/cala-polska?market=ALL&ownerTypeSingleSelect=ALL&viewType=listing&lang=pl&searchingCriteria=wynajem&searchingCriteria=pokoj&page={}&limit=400'.format(page)
    req = requests.get(room_url).text
    room_rent_htmls = room_rent_htmls + req[:-7]

#Empty arrays to store data
fs_post_titles, fs_post_prices, fs_post_cities, fs_post_sqmetrage, fs_post_rooms, fs_post_type, \
fr_post_titles, fr_post_prices, fr_post_cities, fr_post_sqmetrage, fr_post_rooms, fr_post_type, \
rr_post_titles, rr_post_prices, rr_post_cities, rr_post_sqmetrage, rr_post_rooms, rr_post_type, \
    = ([] for i in range(18))


#Append data to empty list
def append_data(post, title, price, city, sqmetrage, rooms, type):

# 1. Find & append titles
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
    title.append(post_title)

# 2. Find & append prices, number of rooms, square metrage (they're in same span class).
    # Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned.     
    post_price = post.find('span', class_ = 'css-rmqm02 eclomwz0').text
    post_price_ns = unicodedata.normalize('NFKD', post_price)

    post_price_replace = {' ':'', 'zł': '','€':'','$':'', ',':'.'}
    for key, value in post_price_replace.items():
        post_price_ns = post_price_ns.replace(key, value)

    if post_price_ns == 'Zapytajocenę':
        price.append(None)
    else:
        price.append(round(int(float(post_price_ns)), 0))

# 3. Find & append cities in common class span. Comparing with goverment list of polish cities.
    post_area = post.find('span', class_ = 'css-17o293g es62z2j9').text.split(',')
    for town in post_area:
        if town.strip() in cities_list:
            city.append(town.strip())
            break
        else:
            continue

# 4/5. Find & append square metrage and rooms in common class span.
    post_sq_rooms = post.find_all('span', class_='css-rmqm02 eclomwz0')
    rooms.append(int(post_sq_rooms[2].text[0]))
    sqmetrage.append(float(post_sq_rooms[3].text.split(' ')[0]))

# 6. Find & append type of post. Can be private or company.
    post_sq_type = post.find('span', class_='css-16zp76g e1dxhs6v2')
    if post_sq_type == 'Oferta prywatna':
        type.append('Private post')
    else:
        type.append('Company post')


# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table
flat_sale_soup = BeautifulSoup(flat_sale_htmls, 'lxml')
flat_rent_soup = BeautifulSoup(flat_rent_htmls, 'lxml')
room_rent_soup = BeautifulSoup(room_rent_htmls, 'lxml')

fs_post_container = flat_sale_soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')
fr_post_container = flat_rent_soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')
rr_post_container = room_rent_soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')

# Loop to dive into post container and extract informations. Set n_jobs to -1 and it will use all CPU from device.
if __name__ == '__main__':
    Parallel(n_jobs=1)(delayed(append_data)(post, fr_post_titles, fr_post_prices, fr_post_cities, fr_post_sqmetrage, fr_post_rooms, fr_post_type) for post in fs_post_container)
    Parallel(n_jobs=1)(delayed(append_data)(post, fs_post_titles, fs_post_prices, fs_post_cities, fs_post_sqmetrage, fs_post_rooms, fs_post_type) for post in fs_post_container)
    Parallel(n_jobs=1)(delayed(append_data)(post, rr_post_titles, rr_post_prices, rr_post_cities, rr_post_sqmetrage, rr_post_rooms, rr_post_type) for post in fs_post_container)

# Create dictionary from lists
fr_posts_dict = [{'post_title': post_titles, 'post_price': post_prices, 'post_city': post_cities,
               'post_sqmetrage': post_sqmetrage, 'post_rooms': post_rooms, 'post_type': post_type}
                for post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type
                in zip(fr_post_titles, fr_post_prices, fr_post_cities, fr_post_sqmetrage, fr_post_rooms, fr_post_type)]

# Start spark session, create dataframe from lists and add new conditional columns
spark = SparkSession.builder.getOrCreate()

posts_df = spark.createDataFrame(fr_posts_dict)

modified_posts_df = posts_df.select(['*', \
        \
        (when((posts_df.post_sqmetrage < 30), lit('<30')) \
        .when((posts_df.post_sqmetrage >= 30) & (posts_df.post_sqmetrage <50), lit('30-49')) \
        .when((posts_df.post_sqmetrage >= 50) & (posts_df.post_sqmetrage <75), lit('50-74')) \
        .when((posts_df.post_sqmetrage >= 75) & (posts_df.post_sqmetrage <= 100), lit('75-100')) \
        .otherwise(lit('>100'))).alias('sqm_bucket'), \
        \
        (when((posts_df.post_title.contains('pilne')) | (posts_df.post_title.contains('pilnie')), lit('urgent')) \
        .otherwise(lit('normal'))).alias('urgency'), \
        \
        lit((posts_df.post_price) / (posts_df.post_sqmetrage)).alias('price_per_sqm'), \
        \
        lit(current_date()).alias('date') \
        ])

# Save Spark df to MySQL
modified_posts_df.write \
                    .format("jdbc") \
                    .option("url","jdbc:mysql://localhost/properties") \
                    .option("dbtable","mieszkania") \
                    .option("user", mysql_username) \
                    .option("password", mysql_password) \
                    .mode('Append') \
                    .save()


