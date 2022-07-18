from bs4 import BeautifulSoup
import requests

text_html = requests.get('https://www.otodom.pl/pl/oferty/sprzedaz/mieszkanie/cala-polska?market=ALL&viewType=listing&lang=pl&searchingCriteria=sprzedaz&searchingCriteria=mieszkanie').text
soup = BeautifulSoup(text_html, 'html.parser')

post_container = soup.find('article', class_ = 'css-1th7s4x es62z2j16')
post_title = post_container.find('h3', class_ = 'css-1rhznz4 es62z2j11').text
post_price = post_container.find('span', class_ = 'css-rmqm02 eclomwz0').text
print(post_title)
print(post_price)