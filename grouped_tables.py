import pyspark
from pyspark.sql import SparkSession, functions as pyspf
from pyspark.sql.functions import lit, current_date
import os

mysql_username = os.environ.get('MYSQL_USERNAME')
mysql_password = os.environ.get('MYSQL_PASSWORD')

spark = SparkSession.builder.master('local').getOrCreate()

# Read MySQL table with raw apartaments data into spark df
all_data_df = spark.read.format('jdbc') \
                    .option('url', 'jdbc:mysql://localhost/properties') \
                    .option('driver', 'com.mysql.jdbc.Driver') \
                    .option("user", mysql_username) \
                    .option("password", mysql_password) \
                    .option("dbtable","mieszkania") \
                    .load()

# Df with data grouped by city and post type (rent/sale). Details.
city_grouped = all_data_df.groupBy('post_city', 'rodzaj').agg(pyspf.round(pyspf.avg('post_price'), 0).alias('avg_price'), pyspf.min('post_price').alias('min_price'), pyspf.max('post_price').alias('max_price'), \
                                            pyspf.round(pyspf.avg('post_rooms'), 0).alias('avg_rooms'), \
                                            pyspf.round(pyspf.avg('post_sqmetrage'), 0).alias('avg_sqmetrage'), pyspf.min('post_sqmetrage').alias('min_sqmetrage'), pyspf.max('post_sqmetrage').alias('max_sqmetrage'), \
                                            pyspf.round(pyspf.avg('price_per_sqm'), 0).alias('avg_price_sqm'), pyspf.min('price_per_sqm').alias('min_price_sqm'), pyspf.max('price_per_sqm').alias('max_price_sqm') \
                                            ).withColumn('download_date', lit(current_date()))


# List of main cities in PL. Df with city_grouped df filtered by them.

main_cities_list = ['Białystok', 'Bydgoszcz', 'Gdańsk', 'Gorzów Wielkopolski', 'Katowice', 'Kielce', 'Kraków', 'Lublin', 'Łódź', 'Olsztyn', 'Opole', 'Poznań', 
                    'Rzeszów', 'Szczecin', 'Toruń', 'Warszawa', 'Wrocław', 'Zielona Góra']

main_cities_grouped = city_grouped.filter(city_grouped.post_city.isin(main_cities_list))

# Grupowanie w koszyki ceny, ceny za metr