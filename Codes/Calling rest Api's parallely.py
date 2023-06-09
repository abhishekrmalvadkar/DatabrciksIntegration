# Databricks notebook source


# COMMAND ----------

import requests
import concurrent.futures
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create the schemas for both the datasets [ cars and planes ]


schemas = [
    StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("Model", StringType(), nullable=True),
    StructField("Price", StringType(), nullable=True),
    StructField("ToCity", StringType(), nullable=True),
    StructField("Company", StringType(), nullable=True)
   #above is the planes schema 
]),
    StructType([
    StructField("id", IntegerType(), nullable = True),
    StructField("Color", StringType(), nullable = True),
    StructField("Model", StringType(), nullable = True),
    StructField("Price", StringType(), nullable = True),
    StructField("Company", StringType(), nullable = True)
    #above is the cars schema
])
]

#below are the two urls which have been generated by manually uploading the csv files and creating the rest api links
urls = ['https://retoolapi.dev/lc90fd/data','https://retoolapi.dev/i4c9po/data']

#create a function called fetch_data which gets the data from the urls using requests module and stores the data in response object.
#the response.json() converts the python object to json data
def fetch_data (url):
    response = requests.get(url)
    return response.json()

#concurrent.futures.ThreadPoolExecutor() is used to create a pool of threads for parallel execution
#The ThreadPoolExecutor is used to submit API requests concurrently, and the responses are collected in the api_responses list.
#Inside the ThreadPoolExecutor, the fetch_data() function is submitted for each URL using executor.submit(), which returns a #future object representing the asynchronous execution of the function. The futures are collected in the futures list.
#The concurrent.futures.as_completed() function is used to iterate over the completed futures.
#The results of the completed futures (i.e., the API responses) are stored in the api_responses list.
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(fetch_data, url) for url in urls]
    api_responses = [future.result() for future in concurrent.futures.as_completed(futures)]

#the zip function is used to iterate over two lists simultaneously. Here the Zip function pairs each value of api_response with the schemas defined

dfs = []
for response, schema in zip(api_responses, schemas):
    df = spark.createDataFrame(response, schema)
    dfs.append(df)

for i, df in enumerate(dfs):
    print(f"Data fetched from API {i+1}:")
    df.show(n=30)






# COMMAND ----------

df_merged = dfs[0].alias('df1').join(dfs[1].alias('df2'), col('df1.id') == col('df2.id'))
df_merged = df_merged.filter(col('df1.id') != 6)


# COMMAND ----------

df_merged.show()

# COMMAND ----------

import requests

def delete(url):
    response = requests.delete(url)
    return response

urls = ["https://retoolapi.dev/mOFM9k/data/20", "https://retoolapi.dev/mOFM9k/data/21"]

for url in urls:
    response = delete(url)
    print(f"DELETE {url} - Status code: {response.status_code}")


# COMMAND ----------


