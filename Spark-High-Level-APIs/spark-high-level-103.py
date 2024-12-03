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

orders_df = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/public/trendytech/orders_wh/*")

# create a database if not exists already
spark.sql("create database if not exists itv011877_retail")

# list the existing databases
spark.sql("show databases").show()

# check the database, which was created by me
spark.sql("show databases").filter("namespaces = 'itv011877_retail'").show()

# check the databases starting with particular pattern
spark.sql("show databases").filter("namespaces like 'itv011877%'").show()
spark.sql("show databases").filter("namespaces like 'retail%'").show()

# list the existing tables in a database
spark.sql("show tables").show()

# switch to the specific database
spark.sql("itv011877_retail")

# create a table in your database
spark.sql("create table itv011877_retail.orders (order_id integer, order_date string, customer_id integer, order_status string)")
spark.sql("show tables")

# loading the data from the temp_view to the persistent table
spark.sql("insert into itv011877_retail.orders select * from orders")
spark.sql("select * from itv011877_retail.orders limit 5").show()

# describing the table
spark.sql("describe itv011877_retail.orders").show()
spark.sql("describe extended itv011877_retail.orders").show(truncate = False)
spark.sql("describe formatted itv011877_retail.orders").show(truncate = False)

# dropping the table
spark.sql("drop table itv011877_retail.orders")
       