# Databricks notebook source
# DBTITLE 1,mount transaction container
dbutils.fs.mount(source = 'wasbs://transaction@demostorageabhishek.blob.core.windows.net' ,
                 mount_point = '/mnt/transaction1',
                 extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='})

# COMMAND ----------

#dbutils.fs.unmount('/mnt/output')

# COMMAND ----------

# DBTITLE 1,mount customertype container
dbutils.fs.mount(source = 'wasbs://customertype@demostorageabhishek.blob.core.windows.net' ,
                 mount_point = '/mnt/customertype1',
                 extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='})

# COMMAND ----------

# DBTITLE 1,mount path for output file
dbutils.fs.mount(source = 'wasbs://customertypeandtransaction@demostorageabhishek.blob.core.windows.net' ,
                 mount_point = '/mnt/output',
                 extra_configs = {'fs.azure.account.key.demostorageabhishek.blob.core.windows.net':'pcy7mEao6FT6zttzYIaEJQ9HFJ5g4fG2XIIY8tQcwj/TKya0HxBN92WO5iESUfESCT4xABfUcwlt+AStIlFBFQ=='})

# COMMAND ----------

# DBTITLE 1,Create schema and read the csv files
from pyspark.sql.types import *
from pyspark.sql.functions import *

transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), nullable=False),
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("price", DoubleType(), nullable=False)
])

df_read_transaction = spark.read.format('csv').option('header', True).schema(transactions_schema).load('/mnt/transaction1')

customerType_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("customer_type", StringType(), nullable=False)
])

df_read_customerType = spark.read.format('csv').option('header', True).schema(customerType_schema).load('/mnt/customertype1')

# COMMAND ----------

df_read_transaction.show()
df_read_customerType.show()


# COMMAND ----------

# DBTITLE 1,Join the customer and transaction data
df_join = df_read_transaction.join(df_read_customerType, 'customer_id')
df_join.show()

# COMMAND ----------

# DBTITLE 1,Calculate total revenue
total_revenue = df_join.withColumn("total_Revenue", col('quantity')*col('price'))
total_revenue.show()

# COMMAND ----------

# DBTITLE 1,Apply discounts as per the customer_type
df_discount = total_revenue.withColumn('discount_price',when(col('customer_type')=='regular', col('total_Revenue') * 0.9 )\
                .when(col('customer_type')=='premium', col('total_Revenue') * 0.8 )\
                .when(col('customer_type')=='vip', col('total_Revenue') * 0.7 ))
df_discount.show()

# COMMAND ----------

# DBTITLE 1,calculate average discount based on customer_type
average_discount = df_discount.groupBy('customer_type').avg('discount_price')
average_discount.show()

# COMMAND ----------

df_discount.write.csv("/mnt/output", header = True, mode = "overwrite")

# COMMAND ----------


