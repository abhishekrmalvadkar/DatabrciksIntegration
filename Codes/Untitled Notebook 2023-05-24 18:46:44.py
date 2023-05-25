# Databricks notebook source
df = spark.read.format("com.databricks.spark.xml").option("rootTag","catalog").option("rowTag","book").load("/dbfs/FileStore/books.xml")
df.show()


# COMMAND ----------


