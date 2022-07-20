from bs4 import BeautifulSoup
import requests
import pandas as pd
import unicodedata
import csv

# Connecting to otodom list cities to create list of available cities. Based on that we can later exctract city from common class which include other strings.
with open('cities.csv', encoding="utf8") as cities_file:
    reader = csv.reader(cities_file)
    cities = list(reader)
    cities_list = []
    for city_listed in cities:
        for city in city_listed:
            if city != '':
                cities_list.append(city)
    


# Connect to site, create soup
main_site_html = requests.get('https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie').text
main_soup = BeautifulSoup(main_site_html, 'html.parser')

# Post containers and empty lists to which we're appending info scrapped. Later we will use the lists to create pandas table
post_container = main_soup.find_all('article', class_ = 'css-1th7s4x es62z2j16')
post_titles, post_prices, post_cities, post_sqmetrage, post_rooms, post_type  = ([] for i in range(6))

# Loop to dive into post container and extract informations
for post in post_container:
    # 1. Find & append titles
    post_title = post.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
    post_titles.append(post_title)

    # 2. Find & append prices, number of rooms, square metrage (they're in same span class).
    # Loop for replacements in price. Inserting None instead of "ask for price" if there's no price mentioned. 
    post_price = post.find('span', class_ = 'css-rmqm02 eclomwz0').text
    post_price_ns = unicodedata.normalize('NFKD', post_price)

    post_price_replace = {' ':'', 'zł': '', ',':'.'}
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


print(len(post_titles))
print(len(post_cities))
print(len(post_prices))

 