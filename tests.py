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


sys.setrecursionlimit(20000)

#Setting environmental variables for db credentials
mysql_username = os.environ.get('MYSQL_USERNAME')
mysql_password = os.environ.get('MYSQL_PASSWORD')

print('SPARK_HOME' in os.environ)