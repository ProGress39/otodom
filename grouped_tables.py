import pyspark
from pyspark.sql import SparkSession, functions as pyspf
from pyspark.sql.functions import lit, current_date
import os
from decouple import config

def run_summary_spark():

    mysql_username = config('USER')
    mysql_password = config('KEY')

    spark = SparkSession.builder.master('local').getOrCreate()

    # Read MySQL table with raw apartaments data into spark df
    all_data_df = spark.read.format('jdbc') \
                        .option("url","jdbc:mysql://localhost/apartments_web_scrapper") \
                        .option('driver', 'com.mysql.jdbc.Driver') \
                        .option("user", mysql_username) \
                        .option("password", mysql_password) \
                        .option("dbtable","mieszkania") \
                        .load()

    # Df with data grouped by city and post type (rent/sale). Details. Send to MySQL database. Filtered by current date to append only current date aggregations.
    city_grouped = all_data_df.filter(all_data_df.download_date == current_date()).groupBy('post_city', 'type').agg(pyspf.round(pyspf.avg('post_price'), 0).alias('avg_price'), pyspf.min('post_price').alias('min_price'), pyspf.max('post_price').alias('max_price'), \
                                                pyspf.round(pyspf.avg('post_rooms'), 0).alias('avg_rooms'), \
                                                pyspf.round(pyspf.avg('post_sqmetrage'), 0).alias('avg_sqmetrage'), pyspf.min('post_sqmetrage').alias('min_sqmetrage'), pyspf.max('post_sqmetrage').alias('max_sqmetrage'), \
                                                pyspf.round(pyspf.avg('price_per_sqm'), 0).alias('avg_price_sqm'), pyspf.min('price_per_sqm').alias('min_price_sqm'), pyspf.max('price_per_sqm').alias('max_price_sqm'), \
                                                pyspf.count(pyspf.lit(1)).alias('number_of_posts')
                                                ).withColumn('download_date', lit(current_date()))
                                                

    city_grouped.write \
                        .format("jdbc") \
                        .option("url","jdbc:mysql://localhost/apartments_web_scrapper") \
                        .option("driver", "com.mysql.jdbc.Driver") \
                        .option("dbtable","city_summary") \
                        .option("user", mysql_username) \
                        .option("password", mysql_password) \
                        .mode('Append') \
                        .save()


    # List of main cities in PL. Df with city_grouped df filtered by them. Filtered by current date to append only current date aggregations.

    main_cities_list = ['Białystok', 'Bydgoszcz', 'Gdańsk', 'Gorzów Wielkopolski', 'Katowice', 'Kielce', 'Kraków', 'Lublin', 'Łódź', 'Olsztyn', 'Opole', 'Poznań', 
                        'Rzeszów', 'Szczecin', 'Toruń', 'Warszawa', 'Wrocław', 'Zielona Góra']

    main_cities_grouped = city_grouped.filter(city_grouped.post_city.isin(main_cities_list))

    main_cities_grouped.write \
                        .format("jdbc") \
                        .option("url","jdbc:mysql://localhost/apartments_web_scrapper") \
                        .option("driver", "com.mysql.jdbc.Driver") \
                        .option("dbtable","main_cities_summary") \
                        .option("user", mysql_username) \
                        .option("password", mysql_password) \
                        .mode('Append') \
                        .save()