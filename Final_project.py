#############################################################
####### code that save the data locally in the machine #######
#############################################################

import json
import requests
from os import makedirs
from os.path import join, exists
from datetime import date, timedelta

# ARTICLES_DIR = join('tempdata', 'articles')
ARTICLES_DIR = '/tmp/staging/final_project/'
makedirs(ARTICLES_DIR, exist_ok=True)

MY_API_KEY = 'a3de7f46-804c-43da-993a-ac260cd6acb2'
# MY_API_KEY = open("creds_guardian.txt").read().strip()
API_ENDPOINT = 'http://content.guardianapis.com/search'
my_params = {
    'from-date': "",
    'to-date': "",
    'order-by': "newest",
    'show-fields': 'all',
    'page-size': 200,
    'api-key': MY_API_KEY
}

# day iteration from here:
# http://stackoverflow.com/questions/7274267/print-all-day-dates-between-two-dates
start_date = date(2022, 1, 15)
end_date = date(2022, 1, 15)
dayrange = range((end_date - start_date).days + 1)
for daycount in dayrange:
    dt = start_date + timedelta(days=daycount)
    datestr = dt.strftime('%Y-%m-%d')
    fname = join(ARTICLES_DIR, datestr + '.json')
    if not exists(fname):
        # then let's download it
        print("Downloading", datestr)
        all_results = []
        my_params['from-date'] = datestr
        my_params['to-date'] = datestr
        current_page = 1
        total_pages = 1
        while current_page <= total_pages:
            print("...page", current_page)
            my_params['page'] = current_page
            resp = requests.get(API_ENDPOINT, my_params)
            data = resp.json()
            all_results.extend(data['response']['results'])
            # if there is more than one page
            current_page += 1
            total_pages = data['response']['pages']

        with open(fname, 'w') as f:
            print("Writing to", fname)

            # re-serialize it for pretty indentation
            f.write(json.dumps(all_results))

#############################################################
####### copy files from local machine into HDFS       #######
#############################################################

import os
import pyarrow as pa

fs = pa.hdfs.connect(
    host='cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

# First we use mkdir() to create a staging area in HDFS under /tmp/sqoop/staging.
fs.mkdir('hdfs://cnt7-naya-cdh63:8020/tmp/staging/final_project', create_parents=True)
local_path = '/tmp/staging/final_project/'+datestr+'.json'
with open(local_path, 'rb') as f:
    fs.upload('hdfs://cnt7-naya-cdh63:8020/tmp/staging/final_project/'+datestr+'.json', f)

#############################################################
####### Read files from hdfs with spark         #############
####### transform it to data frame              #############
####### send the result to sentiment analysis   #############
####### save df as json in local machine        #############
#############################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, StringType, explode

spark = SparkSession.builder.getOrCreate()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType, FloatType, ArrayType, StructField
from textblob import TextBlob

df = spark.read.json("hdfs://cnt7-naya-cdh63:8020/tmp/staging/final_project/"+datestr+".json")

# df.select(col('id'), col('sectionId'), col('sectionName'), col('webPublicationDate'), col('fields.headline'), col('fields.bodyText'), col('fields.wordcount'), col('fields.shortUrl'), col('fields.productionOffice'))\
#     .show()
df = df.select(col('id'), col('sectionId'), col('sectionName'), col('webPublicationDate'), col('fields.headline'),
               col('fields.bodyText'), col('fields.wordcount'), col('fields.shortUrl'), col('fields.productionOffice'))

# Filter articles that have more than 10 words
df = df.where(col("wordcount") > 10)


# Add sentiment analysis in column "wordCount"
def get_sentiment(string1):
    return TextBlob(string1).sentiment.polarity


get_sentiment_udf = udf(get_sentiment, FloatType())
df = df.withColumn('sentiment', get_sentiment_udf(col('bodyText')))

# save the df as json
df.write.json("/tmp/staging/output_news_sentiment/"+datestr+".json")

# print(df.show(5))
