from bs4 import BeautifulSoup
import lxml.html
import os
import sys
import pandas as pd
import csv
import requests
import unicodedata
from joblib import Parallel, delayed
import joblib
import pickle
from pyspark.sql.functions import when, lit, current_date
import pyspark
from pyspark.sql import SparkSession, DataFrameWriter
from decouple import config

def run_olx_scrapper():
    
    sys.setrecursionlimit(20000)

    #Setting environmental variables for db credentials
    mysql_username = config('USER')
    mysql_password = config('KEY')

    # Solving problem with PySpark environmental variables
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

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
    fs_post_container = BeautifulSoup(flat_sale_htmls, 'lxml').find_all('div', class_ = ['css-1apmciz'])
    fr_post_container = BeautifulSoup(flat_rent_htmls, 'lxml').find_all('div', class_ = ['css-1apmciz'])
    rr_post_container = BeautifulSoup(room_rent_htmls, 'lxml').find_all('div', class_ = ['css-1apmciz'])

    #Append data to empty list function
    def append_data(post, title, price, city, sq_metrage):

    # 1. Find & append titles
        post_title = post.find('h6', class_ = 'css-v3vynn-Text eu5v0x0').text
        title.append(post_title)

    # 2. Find & append prices
    # Loop for replacements in price.
        post_price = post.find('p', class_ = 'css-wpfvmn-Text eu5v0x0').text
        post_price_ns = unicodedata.normalize('NFKD', post_price)

        post_price_replace = {' ':'', 'zł': '', 'donegocjacji' : '', ",":"."}
        for key, value in post_price_replace.items():
            post_price_ns = post_price_ns.replace(key, value)

        price.append(round(float(post_price_ns), 2))


    # 3. Find & append cities in common class span. Comparing with goverment list of polish cities.
        post_area = post.find('p', class_ = 'css-p6wsjo-Text eu5v0x0').text
        element = post_area.split(',')[0]
        city.append(element.split('-')[0].strip())


    # 4/5. Find square metrage in common class span.
        post_sqm = post.find('p', class_='css-1bhbxl1-Text eu5v0x0').text[:-3].replace(",", ".")
        try:
            sq_metrage.append(int(round(float(post_sqm),0)))
        except:
            sq_metrage.append(None)


    #Empty lists to store details data, 5 for every type of post (rent, sale, room rent)
    post_details = []

    for i in range(12):
        post_details.append([])

    # Loop to dive into post container and extract informations. Set n_jobs to -1 and it will use all CPU from device. [3:] to skip promoted offers
    if __name__ == '__main__':
        Parallel(n_jobs=1)(delayed(append_data)(post, post_details[0], post_details[1], post_details[2], post_details[3]) for post in fr_post_container)
        Parallel(n_jobs=1)(delayed(append_data)(post, post_details[4], post_details[5], post_details[6], post_details[7]) for post in fs_post_container)
        Parallel(n_jobs=1)(delayed(append_data)(post, post_details[8], post_details[9], post_details[10], post_details[11]) for post in rr_post_container)

    # Create dictionaries from lists
    fr_posts_dict = [{'post_title': post_titles, 'post_price': post_prices, 'post_city': post_cities,
                'post_sqmetrage': post_sqmetrage}
                    for post_titles, post_prices, post_cities, post_sqmetrage
                    in zip(post_details[0], post_details[1], post_details[2], post_details[3])]

    fs_posts_dict = [{'post_title': post_titles, 'post_price': post_prices, 'post_city': post_cities,
                'post_sqmetrage': post_sqmetrage}
                    for post_titles, post_prices, post_cities, post_sqmetrage
                    in zip(post_details[4], post_details[5], post_details[6], post_details[7])]

    rr_posts_dict = [{'post_title': post_titles, 'post_price': post_prices, 'post_city': post_cities}
                    for post_titles, post_prices, post_cities
                    in zip(post_details[8], post_details[9], post_details[10])]

    # Start spark session, create dataframes from lists and add new conditional columns. Could use pandas here, but project is for CV, so want to show Spark basic knowledge.
    # Save dataframes to local MySQL database

    spark = SparkSession.builder.getOrCreate()

    fr_post_container = spark.createDataFrame(fr_posts_dict)

    m_fr_post_container = fr_post_container.select(['*', \
            \
            (when((fr_post_container.post_sqmetrage < 30), lit('<30')) \
            .when((fr_post_container.post_sqmetrage >= 30) & (fr_post_container.post_sqmetrage <50), lit('30-49')) \
            .when((fr_post_container.post_sqmetrage >= 50) & (fr_post_container.post_sqmetrage <75), lit('50-74')) \
            .when((fr_post_container.post_sqmetrage >= 75) & (fr_post_container.post_sqmetrage <= 100), lit('75-100')) \
            .otherwise(lit('>100'))).alias('sqm_bucket'), \
            \
            (when((fr_post_container.post_title.contains('pilne')) | (fr_post_container.post_title.contains('pilnie')), lit('urgent')) \
            .otherwise(lit('normal'))).alias('urgency'), \
            \
            lit((fr_post_container.post_price) / (fr_post_container.post_sqmetrage)).alias('price_per_sqm'), \
            \
            lit(current_date()).alias('download_date'), \
            lit('rent').alias('type'), \
            lit('olx').alias('source') \
            ])

    m_fr_post_container.write \
                        .format("jdbc") \
                        .option("url","jdbc:mysql://localhost/apartments_web_scrapper") \
                        .option("driver", "com.mysql.jdbc.Driver") \
                        .option("dbtable","mieszkania") \
                        .option("user", mysql_username) \
                        .option("password", mysql_password) \
                        .mode('Append') \
                        .save()


    fs_post_container = spark.createDataFrame(fs_posts_dict)
    m_fs_post_container = fs_post_container.select(['*', \
            \
            (when((fs_post_container.post_sqmetrage < 30), lit('<30')) \
            .when((fs_post_container.post_sqmetrage >= 30) & (fs_post_container.post_sqmetrage <50), lit('30-49')) \
            .when((fs_post_container.post_sqmetrage >= 50) & (fs_post_container.post_sqmetrage <75), lit('50-74')) \
            .when((fs_post_container.post_sqmetrage >= 75) & (fs_post_container.post_sqmetrage <= 100), lit('75-100')) \
            .otherwise(lit('>100'))).alias('sqm_bucket'), \
            \
            (when((fs_post_container.post_title.contains('pilne')) | (fs_post_container.post_title.contains('pilnie')), lit('urgent')) \
            .otherwise(lit('normal'))).alias('urgency'), \
            \
            lit((fs_post_container.post_price) / (fs_post_container.post_sqmetrage)).alias('price_per_sqm'), \
            \
            lit(current_date()).alias('download_date'), \
            lit('sell').alias('type'), \
            lit('olx').alias('source') \
            ])

    m_fs_post_container.write \
                        .format("jdbc") \
                        .option("url","jdbc:mysql://localhost/apartments_web_scrapper") \
                        .option("driver", "com.mysql.jdbc.Driver") \
                        .option("dbtable","mieszkania") \
                        .option("user", mysql_username) \
                        .option("password", mysql_password) \
                        .mode('Append') \
                        .save()


    rr_post_container = spark.createDataFrame(rr_posts_dict)
    m_rr_post_container = rr_post_container.select(['*', \
            \
            lit(0).alias('sqm_bucket'), \
            \
            (when((rr_post_container.post_title.contains('pilne')) | (rr_post_container.post_title.contains('pilnie')), lit('urgent')) \
            .otherwise(lit('normal'))).alias('urgency'), \
            \
            lit(0).alias('price_per_sqm'), \
            \
            lit(current_date()).alias('download_date'), \
            lit('room').alias('type'), \
            lit('olx').alias('source') \
            ])

    m_rr_post_container.write \
                        .format("jdbc") \
                        .option("url","jdbc:mysql://localhost/apartments_web_scrapper") \
                        .option("driver", "com.mysql.jdbc.Driver") \
                        .option("dbtable","mieszkania") \
                        .option("user", mysql_username) \
                        .option("password", mysql_password) \
                        .mode('Append') \
                        .save()
        

