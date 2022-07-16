from matplotlib.pyplot import text
import requests

text_html = requests.get('https://www.olx.pl/d/motoryzacja/samochody/?search%5Bfilter_float_price:from%5D=10000&search%5Bfilter_float_price:to%5D=50000&search%5Bfilter_float_year:from%5D=2010')

print(text_html)