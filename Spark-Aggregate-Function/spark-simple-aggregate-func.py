from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

spark = SparkSession. \
builder. \
config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
enableHiveSupport(). \
master('yarn'). \
getOrCreate()

orders_df = spark.read \
.format('csv') \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/public/trendytech/datasets/orders_data.csv")

orders_df.show()

from pyspark.sql.functions import *

# How many total records are there in the dataframe 'orders_df'
orders_df.select(count(*).alias("row_count")).show()

# Note: count() function counts the rows even if there is a null.

# Find the number of distinct invoice ids in the dataframe 'orders_df'
orders_df.select(countDistinct("invoiceno").alias("unique_invoice_no")).show()

# Sum of quantities
orders_df.select(sum("quantity").alias("total_quantity")).show()

# Find the average unit price
orders_df.select(avg("unitprice").alias("avg_price")).show()

# Programmatic Way
orders_df.select(count(*).alias("row_count"), countDistinct("invoiceno").alias("unique_invoice_no"), sum("quantity").alias("total_quantity"), avg("unitprice").alias("avg_price")).show()

# Using column expression
orders_df.selectExpr("count(*) as row_count", "count(distinct(invoiceno)) as unique_invoice_no", "sum(quantity) as total_quantity", "avg(unitprice) as avg_price").show()

# Using sql way
orders_df.createOrReplaceTempView("orders")
spark.sql("select count(*) as row count, count(distinct(invoiceno)) as unique_invoice_no, sum(quantity) as total_quantity, avg(unitprice) as avg_price from orders").show()






