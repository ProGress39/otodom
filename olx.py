from bs4 import BeautifulSoup
import lxml.html
import os
import sys
import pandas as pd
import csv
import requests
import unicodedata

sys.setrecursionlimit(20000)

#Setting environmental variables for db credentials
mysql_username = os.environ.get('MYSQL_USERNAME')
mysql_password = os.environ.get('MYSQL_PASSWORD')

# Connect to olx webpage and append htmls to strings. Seperate variables for sales, rents and room rents
flat_sale_htmls, flat_rent_htmls, room_rent_htmls = ('' for i in range(3))

for page in range(2, 3):
    sale_url = 'https://www.olx.pl/d/nieruchomosci/mieszkania/wynajem/?page={}'.format(page)
    rent_url = 'https://www.olx.pl/d/nieruchomosci/mieszkania/wynajem/?page={}'.format(page)
    room_url = 'https://www.olx.pl/d/nieruchomosci/stancje-pokoje/?page={}'.format(page)

    # Rooms & rents have much less pages of posts so we need to manage it with tryexcept within sales pages range.
    flat_sale_htmls = flat_sale_htmls + requests.get(sale_url).text[:-7] #-7 to remove </html> as lxml parser doesn't work properly with it.
    try:
        flat_rent_htmls = flat_rent_htmls + requests.get(rent_url).text[:-7]
    except:
        pass

    try:
        room_rent_htmls = room_rent_htmls + requests.get(room_url).text[:-7]
    except:
        pass


# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table.
fs_post_container = BeautifulSoup(flat_sale_htmls, 'lxml').find_all('div', class_ = ['css-9nzgu8'])
fr_post_container = BeautifulSoup(flat_rent_htmls, 'lxml').find_all('div', class_ = ['css-9nzgu8'])
rr_post_container = BeautifulSoup(room_rent_htmls, 'lxml').find_all('div', class_ = ['css-9nzgu8'])

#Append data to empty list function
def append_data(post, title, price, city):

# 1. Find & append titles
    post_title = post.find('h6', class_ = 'css-v3vynn-Text eu5v0x0').text
    title.append(post_title)

# 2. Find & append prices, number of rooms, square metrage (they're in same span class).
    # Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned.     
    post_price = post.find('p', class_ = 'css-wpfvmn-Text eu5v0x0').text
    post_price_ns = unicodedata.normalize('NFKD', post_price)

    post_price_replace = {' ':'', 'zł': '', 'donegocjacji' : ''}
    for key, value in post_price_replace.items():
        post_price_ns = post_price_ns.replace(key, value)

    price.append(round(int(float(post_price_ns)), 0))


# 3. Find & append cities in common class span. Comparing with goverment list of polish cities.
    post_area = post.find('p', class_ = 'css-p6wsjo-Text eu5v0x0').text
    element = post_area.split(',')[0]
    city.append(element.split('-')[0].strip())
    
    


title = []
price = []
city = []

for post in fr_post_container:
    append_data(post, title, price, city)

print(len(title))
print(len(price))
print(len(city))

