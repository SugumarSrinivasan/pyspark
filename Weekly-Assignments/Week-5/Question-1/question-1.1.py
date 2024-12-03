# importing the required libraries
from pyspark.sql import SparkSession
import getpass

username = getpass.getuser()

# creating the spark session
spark = SparkSession. \
        builder. \
        config('spark.ui.port', '0'). \
        config("spark.sql.warehouse.dir", f"/user/{username}/warehouse"). \
        enableHiveSupport(). \
        master('yarn'). \
        getOrCreate()

# 1.1 Create a managed spark table and load data to it from the given csv file.

# load the data from hdfs to dataframe using standard reader API
groceries_df = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/public/trendytech/groceries.csv")

# create a database
spark.sql("create database itv011877_retail")

# switch into database
spark.sql("use itv011877_retail")

# create a temprorary view from the dataframe
groceries_df.createOrReplaceTempView("groceries")

# create a managed spark table
spark.sql("create table itv011877_retail.groceries (order_id string, location string, item string, order_date string, quantity integer) using csv")

# load the data into managed table from temprorary view 
spark.sql("insert into itv011877_retail.groceries select * from groceries")

# view the data by executing the select statement
spark.sql("select * from itv011877_retail.groceries limit 5").show()

spark.stop()
