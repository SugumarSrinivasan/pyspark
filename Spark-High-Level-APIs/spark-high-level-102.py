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

# standard csv reader
orders_df = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/public/trendytech/orders_wh/*")

# shortcut csv reader
orders_df = spark.read \
.csv("/public/trendytech/orders_wh/*", header = "true", inferSchema = "true")

# standard json reader
orders_df = spark.read \
.format("json") \
.load("/public/trendytech/datasets/orders.json")

#shortcut json reader
orders_df = spark.read \
.json("/public/trendytech/datasets/orders.json")

# standard parquet reader
orders_df = spark.read \
.format("parquet") \
.load("/public/trendytech/datasets/ordersparquet/*")

#shortcut parquet reader
orders_df = spark.read \
.parquet("/public/trendytech/datasets/ordersparquet/*")

# standard orc reader
orders_df = spark.read \
.format("orc") \
.load("/public/trendytech/datasets/ordersorc/*")

#shortcut orc reader
orders_df = spark.read \
.orc("/public/trendytech/datasets/ordersorc/*")

# convert dataframe to spark table
orders_df.createOrReplaceTempView("orders")
filtered_df = spark.sql("SELECT * FROM ORDERS WHERE order_status = 'CLOSED'")
filtered_df.show()

# convert spark table to dataframe
orders_df = spark.read.table("orders")
orders_df.show()