# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://restapi@demostorageabhishek.blob.core.windows.net' ,
                 mount_point = '/mnt/blob3',
                 extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='})

# COMMAND ----------

df = spark.read.json("/mnt/blob3")

df.show()

from pyspark.sql.functions import explode

df_transformed = df.select(df.message, explode(df.data).alias("employee"), df.status)

df_transformed.show()

df_transformed.write.format("delta").option("mergeSchema", True).mode("append").save("/dbfs/mnt/restApi")

df_transformed.show()


# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS silver_table1 USING DELTA LOCATION '/dbfs/mnt/restApi'") 
spark.sql("CREATE TABLE IF NOT EXISTS silver_table2 USING DELTA LOCATION '/dbfs/mnt/restApi'")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table1

# COMMAND ----------

gold_table = spark.sql("SELECT t1.*, t2.* FROM silver_table1 t1 JOIN silver_table2 t2 ON t1.data = t2.data")


# COMMAND ----------

gold_table.display()

# COMMAND ----------


