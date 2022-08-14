import pyspark
from pyspark.sql import SparkSession
import os

mysql_username = os.environ.get('MYSQL_USERNAME')
mysql_password = os.environ.get('MYSQL_PASSWORD')

spark = SparkSession.builder.master('local').getOrCreate()

mysql_connection = spark.read.format('jdbc') \
                    .option('url', 'jdbc:mysql://localhost/properties') \
                    .option('driver', 'com.mysql.jdbc.Driver') \
                    .option("user", mysql_username) \
                    .option("password", mysql_password) \
                    .option("dbtable","mieszkania") \
                    .load()

mysql_connection.show()