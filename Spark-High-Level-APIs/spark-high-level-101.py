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

orders_df.show()

orders_df.printSchema()

transformed_df1 = orders_df.withColumnRenamed("order_status", "status")
transformed_df1.show()

from pyspark.sql.functions import *
transformed_df2 = transformed_df1.withColumn("order_new_date", to_timestamp("order_date"))
transformed_df2.show()

transformed_df2.printSchema()

spark.stop()