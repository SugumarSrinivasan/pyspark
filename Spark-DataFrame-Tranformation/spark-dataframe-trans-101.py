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

# samplingRatio

orders_df = spark.read \
.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.option("samplingRatio", 0.1) \
.load("/public/trendytech/orders_wh/*")
      
orders_df.show()
orders_df.printSchema()

# Type-1: schema DDL 

orders_schema = 'order_id long, order_date date, customer_id long, order_status string'

orders_df = spark.read \
.format("csv") \
.schema("orders_schema") \
.load("/public/trendytech/datasets/orders_samples1.csv")

orders_df.show()
orders_df.printSchema()

# Type-2: StructType & StuctField

from pyspark.sql.types import *

orders_schema_struct = StructType([
StructField("order_id", LongType()),
StructField("order_date", DateType()),
StructField("customer_id", LongType()),
StructField("order_status", StringType())
])

orders_df = spark.read \
.format("csv") \
.schema("orders_schema_struct") \
.load("/public/trendytech/datasets/orders_samples1.csv")

orders_df.show()
orders_df.printSchema()