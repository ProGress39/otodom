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

city_grouped = all_data_df.groupBy('post_city', 'rodzaj').agg(pyspf.round(pyspf.avg('post_price'), 0).alias('avg_price'), pyspf.min('post_price').alias('min_price'), pyspf.max('post_price').alias('max_price'), \
                                            pyspf.round(pyspf.avg('post_rooms'), 0).alias('avg_rooms'), \
                                            pyspf.round(pyspf.avg('post_sqmetrage'), 0).alias('avg_sqmetrage'), pyspf.min('post_sqmetrage').alias('min_sqmetrage'), pyspf.max('post_sqmetrage').alias('max_sqmetrage'), \
                                            pyspf.round(pyspf.avg('price_per_sqm'), 0).alias('avg_price_sqm'), pyspf.min('price_per_sqm').alias('min_price_sqm'), pyspf.max('price_per_sqm').alias('max_price_sqm') \
                                            ).withColumn('download_date', lit(current_date())).show()

# Teraz groupowanie na miasta wojewódzkie


