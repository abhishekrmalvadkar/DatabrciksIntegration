# Databricks notebook source
# DBTITLE 1,JSON REST API
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


# Define the schema for the API response
schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("username", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("phone", StringType(), nullable=True),
    StructField("website", StringType(), nullable=True)
])

#Make API request
df_url = "https://jsonplaceholder.typicode.com/users/1"

df_response = requests.get(df_url)
df_api = df_response.json()

print("Raw API Response:")
print(df_api)

# Process API response
df_apiData = [{
    "id": df_api["id"],
    "name": df_api["name"],
    "username": df_api["username"],
    "email": df_api["email"],
    "phone": df_api["phone"],
    "website": df_api["website"]
}]

# Create PySpark DataFrame from API response
df_apiResponse = spark.createDataFrame(df_apiData, schema)

df_apiResponse.display()


# COMMAND ----------

# DBTITLE 1,Sample CSV file api
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Schema definition
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("Name", StringType(), nullable=True),
    StructField("Department", StringType(), nullable=True),
    StructField("Job Status", StringType(), nullable=True),
    StructField("Joining Date", StringType(), nullable=True),
    StructField("Email Address", StringType(), nullable=True),
    StructField("Monthly Salary", StringType(), nullable=True)
])

# Request the data from URL
df_url = "https://retoolapi.dev/mOFM9k/data"
df_response = requests.get(df_url)
df_api = df_response.json()
print("Raw Data from URL:")
print(df_api)

# Create a DF and link the corresponding schema
df_api_response = spark.createDataFrame(df_api, schema)

df_api_del = df_api_response.drop(col("Job Status"))

print("Data fetched from API")
df_api_del.show()


# COMMAND ----------

# DBTITLE 1,Show all the fields present in JSON data
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json

# Create SparkSession
spark = SparkSession.builder.appName("DataFromURL").getOrCreate()

# Make API request
url = "https://jsonplaceholder.typicode.com/users/1"
response = requests.get(url)
user_data = response.json()

# Define the schema for the user data
schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("username", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("address", StructType([
        StructField("street", StringType(), nullable=True),
        StructField("suite", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("zipcode", StringType(), nullable=True),
        StructField("geo", StructType([
            StructField("lat", StringType(), nullable=True),
            StructField("lng", StringType(), nullable=True)
        ]))
    ])),
    StructField("phone", StringType(), nullable=True),
    StructField("website", StringType(), nullable=True),
    StructField("company", StructType([
        StructField("name", StringType(), nullable=True),
        StructField("catchPhrase", StringType(), nullable=True),
        StructField("bs", StringType(), nullable=True)
    ]))
])

# Create PySpark DataFrame from user data
df_user = spark.createDataFrame([user_data], schema)

# Show the DataFrame
print("User Data from URL:")
df_user.display()


# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Create SparkSession
spark = SparkSession.builder.appName("DataFromURL").getOrCreate()

# Define the schema for the user data
schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("username", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("website", StringType(), nullable=True)
])

# Make API request
url = "https://jsonplaceholder.typicode.com/users/1"
response = requests.get(url)
user_data = response.json()

# Create PySpark DataFrame from user data
df_user = spark.createDataFrame([user_data], schema)

# Show the DataFrame
print("User Data from URL:")
df_user.show()


# COMMAND ----------

# DBTITLE 1,Post Method
import requests

url = "https://retoolapi.dev/mOFM9k/data"
payload = {"name": "John", "email": "john@example.com"}
response = requests.post(url, json=payload)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC POST (Create): The POST method is used to submit data to be processed by the resource identified by the URL. It is typically used to create a new resource on the server. The server determines the URL of the newly created resource and includes it in the response. If the same POST request is made multiple times, it will result in the creation of multiple resources with different URLs.
# MAGIC
# MAGIC PUT (Update): The PUT method is used to update or replace the entire representation of a resource identified by the URL. It sends the complete updated representation of the resource to the server, which replaces the existing resource with the new representation. If the resource does not exist, PUT may create a new resource at the specified URL. Unlike POST, each PUT request to the same URL typically results in the same resource state.
# MAGIC
# MAGIC In summary, POST is used to create a new resource, while PUT is used to update or replace an existing resource.

# COMMAND ----------

# DBTITLE 1,Put Method
import requests

url = "https://retoolapi.dev/mOFM9k/data/1"
payload = {"name": "Abhishek Malvadkar", "email": "124.doe@example.com"}
response = requests.put(url, json=payload)


# COMMAND ----------

# DBTITLE 1,Delete Method
import requests

url = "https://retoolapi.dev/mOFM9k/data/23"  # URL of the record you want to delete

response = requests.delete(url)

if response.status_code == 200:
    print("Record deleted successfully.")
else:
    print(f"Failed to delete record. Status code: {response.status_code}")


# COMMAND ----------

# DBTITLE 1,Patch Method
import requests

url = "https://retoolapi.dev/mOFM9k/data/1"
payload = {"email": "NewEmail@example.com"}
response = requests.patch(url, json=payload)


# COMMAND ----------

# DBTITLE 1,Head and Options Method
import requests

url = "https://example.com"

# Send a HEAD request
response_head = requests.head(url)
print("HEAD Response Headers:")
print(response_head.headers)

# Send an OPTIONS request
response_options = requests.options(url)
print("OPTIONS Response Headers:")
print(response_options.headers)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC OPTIONS:
# MAGIC The OPTIONS method is used to retrieve information about the communication options available for a particular URL or endpoint. When you send an OPTIONS request to a server, it should respond with an HTTP response that includes headers describing the allowed methods, headers, and other information about the resource. This allows the client to determine the available methods and make appropriate requests to the server.
# MAGIC The OPTIONS method is useful for clients to discover the capabilities of a server, including which methods are supported, any authentication requirements, and other relevant information about the resource.
# MAGIC
# MAGIC HEAD:
# MAGIC The HEAD method is similar to the GET method, but it retrieves only the headers of a resource without retrieving the actual content of the resource. It is used when you need to check certain information about a resource, such as the response headers (e.g., Content-Type, Content-Length, Last-Modified) or the status code, without transferring the entire response body.
# MAGIC The HEAD method can be useful in scenarios where you want to determine the availability, metadata, or properties of a resource before deciding whether to retrieve the full content using a GET request. It can help reduce network bandwidth and improve performance when you don't need the actual content.
# MAGIC
# MAGIC Both OPTIONS and HEAD methods are read-only methods and do not modify or change the resource on the server. They provide useful information about the resource and help in making subsequent requests or determining the behavior and capabilities of the server.

# COMMAND ----------


