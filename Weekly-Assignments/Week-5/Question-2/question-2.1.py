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

# 2.1. Find the total number of products in the given dataset.

# load the data using standard reader API
products_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/products/*")

# renaming the columns
column_renamed_df = products_df.withColumnRenamed("_c0", "ProductID").withColumnRenamed("_c1", "Category").withColumnRenamed("_c2", "ProductName").withColumnRenamed("_c3", "Description").withColumnRenamed("_c4", "Price").withColumnRenamed("_c5", "ImageURL")

# finding the total number of products through dataframes way
column_renamed_df.select("productID").count()

# finding the total number of products through spark sql way
column_renamed_df.createOrReplaceTempView("products")
spark.sql("select count(productID) as no_of_products from products").show()

# stopping the spark session
spark.stop()