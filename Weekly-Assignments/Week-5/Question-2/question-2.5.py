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

# 2.5. What are the product names and prices of products that have a price greater than $200 and belong to category 5?

# load the data using standard reader API
products_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/products/*")

# renaming the columns
column_renamed_df = products_df.withColumnRenamed("_c0", "ProductID").withColumnRenamed("_c1", "Category").withColumnRenamed("_c2", "ProductName").withColumnRenamed("_c3", "Description").withColumnRenamed("_c4", "Price").withColumnRenamed("_c5", "ImageURL")

results = column_renamed_df.filter("Category = 5 and Price > 200").select("ProductName", "Price")
results.show(truncate = False)

column_renamed_df.createOrReplaceTempView("products")
results = spark.sql("select ProductName, Price from products where Category = 5 and Price > 200")
results.show(truncate = False)

spark.stop()