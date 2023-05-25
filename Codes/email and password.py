# Databricks notebook source
# DBTITLE 1,Mount the input path
dbutils.fs.mount(source = 'wasbs://may25in@demostorageabhishek.blob.core.windows.net' ,
                 mount_point = '/mnt/may25in',
                 extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='})

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
df_read = spark.read.format('csv').option('header',True).option('delimiter', ';').load('/mnt/may25in')

# COMMAND ----------

df_read.show()

# COMMAND ----------

# DBTITLE 1,Check for null values in Identifier column
df_read.filter(df_read.Identifier.isNull()).show()

# COMMAND ----------

# DBTITLE 1,Remove any NULL values
df_read.na.drop('any').show()

# COMMAND ----------

# DBTITLE 1,Drop duplicates
df_read.dropDuplicates().show()

# COMMAND ----------

# DBTITLE 1,Add Full Name column
df_read = df_read.withColumn("Full_Name", concat(col("First name"),lit(" "),col("Last name")))
df_read.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Some of the most popular encryption algorithms include:
# MAGIC
# MAGIC AES: AES is a symmetric encryption algorithm that uses a 128-bit key to encrypt and decrypt data.
# MAGIC Blowfish: Blowfish is a symmetric encryption algorithm that uses a 64-bit to 448-bit key to encrypt and decrypt data.
# MAGIC RC4: RC4 is a stream cipher that uses a 128-bit key to encrypt and decrypt data.
# MAGIC DES: DES is a symmetric encryption algorithm that uses a 56-bit key to encrypt and decrypt data.
# MAGIC 3DES: 3DES is a symmetric encryption algorithm that uses three 56-bit keys to encrypt and decrypt data.

# COMMAND ----------

# DBTITLE 1,Group the data based on department and location
df_group = df_read.groupBy("Department", "Location").count()
df_group.show()

# COMMAND ----------

# DBTITLE 1,Mount the Output path
dbutils.fs.mount(source = 'wasbs://may25out@demostorageabhishek.blob.core.windows.net' ,
                 mount_point = '/mnt/may25out',
                 extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='})

# COMMAND ----------

# DBTITLE 1,Write the grouped data to a output container
df_group.write.format('csv').option('header', True).mode('overwrite').save('/mnt/may25out/group.csv')

# COMMAND ----------

# DBTITLE 1,Write the transformed data to a output container
df_read.coalesce(1).write.format('csv').option('header', True).mode('overwrite').save('/mnt/may25out/read.csv')

# COMMAND ----------


