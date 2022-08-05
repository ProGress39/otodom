from bs4 import BeautifulSoup
from pyparsing import null_debug_action
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

for page in range(0, 1):
    sale_url = 'https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie&page={}&limit=1000'.format(page)
    rent_url = 'https://www.otodom.pl/pl/oferty/wynajem/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=wynajem&searchingCriteria=mieszkanie&page={}&limit=1000'.format(page)
    room_url = 'https://www.otodom.pl/pl/oferty/wynajem/pokoj/cala-polska?market=ALL&ownerTypeSingleSelect=ALL&viewType=listing&lang=pl&searchingCriteria=wynajem&searchingCriteria=pokoj&page={}&limit=400'.format(page)

    # Rooms & rents have less pages of posts so we need to manage it with tryexcept within sales pages range.
    flat_sale_htmls = flat_sale_htmls + requests.get(sale_url).text[:-7] #-7 to remove </html> as lxml parser doesn't work properly with it.
    try:
        flat_rent_htmls = flat_rent_htmls + requests.get(rent_url).text[:-7]
    except:
        pass

    try:
        room_rent_htmls = room_rent_htmls + requests.get(room_url).text[:-7]
    except:
        pass


#Empty lists to store details data, 6 for every type of post (rent, sale, room rent)
post_details = []

for i in range(18):
    post_details.append([])


#Append data to empty list
def append_data(post, title, price, city, sqmetrage, rooms, type):

# 1. Find & append titles
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j13').text
    title.append(post_title)

# 2. Find & append prices, number of rooms, square metrage (they're in same span class).
    # Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned.     
    post_price = post.find('span', class_ = 'css-s8wpzb eclomwz1').text
    post_price_ns = unicodedata.normalize('NFKD', post_price)

    post_price_replace = {' ':'', 'zł': '','€':'','$':'', ',':'.', '/mc':''}
    for key, value in post_price_replace.items():
        post_price_ns = post_price_ns.replace(key, value)

    if post_price_ns == 'Zapytajocenę':
        price.append(None)
    else:
        price.append(round(int(float(post_price_ns)), 0))

# 3. Find & append cities in common class span. Comparing with goverment list of polish cities.
    post_area = post.find('span', class_ = 'css-17o293g es62z2j11').text.split(',')
    for town in post_area:
        if town.strip() in cities_list:
            city.append(town.strip())
            break
        else:
            continue

# 4/5. Find & append square metrage and rooms in common class span.
    post_sq_rooms = post.find_all('span', class_='css-s8wpzb eclomwz1')
    if len(post_sq_rooms) == 4:
        rooms.append(int(post_sq_rooms[2].text[0]))
        sqmetrage.append(float(post_sq_rooms[3].text.split(' ')[0]))
    elif len(post_sq_rooms) == 3:
        rooms.append(int(post_sq_rooms[1].text[0]))
        sqmetrage.append(float(post_sq_rooms[2].text.split(' ')[0]))
    else:
        rooms.append(0)
        sqmetrage.append(0)

# 6. Find & append type of post. Can be private or company.
    post_sq_type = post.find('span', class_='css-16zp76g e1dxhs6v2')
    if post_sq_type == 'Oferta prywatna':
        type.append('Private post')
    else:
        type.append('Company post')


# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table.
fs_post_container = BeautifulSoup(flat_sale_htmls, 'lxml').find_all('article', class_ = ['css-1rtqihe es62z2j18', 'css-e6zjf7 es62z2j18'])
fr_post_container = BeautifulSoup(flat_rent_htmls, 'lxml').find_all('article', class_ = ['css-1rtqihe es62z2j18', 'css-e6zjf7 es62z2j18'])
rr_post_container = BeautifulSoup(room_rent_htmls, 'lxml').find_all('article', class_ = ['css-1rtqihe es62z2j18', 'css-e6zjf7 es62z2j18'])


# Loop to dive into post container and extract informations. Set n_jobs to -1 and it will use all CPU from device.
if __name__ == '__main__':
    Parallel(n_jobs=1)(delayed(append_data)(post, post_details[0], post_details[1], post_details[2], post_details[3], post_details[4], post_details[5]) for post in fr_post_container)
    Parallel(n_jobs=1)(delayed(append_data)(post, post_details[6], post_details[7], post_details[8], post_details[9], post_details[10], post_details[11]) for post in fs_post_container)
    Parallel(n_jobs=1)(delayed(append_data)(post, post_details[12], post_details[13], post_details[14], post_details[15], post_details[16], post_details[17]) for post in rr_post_container)

print(len(post_details[6]))
print(len(fs_post_container))