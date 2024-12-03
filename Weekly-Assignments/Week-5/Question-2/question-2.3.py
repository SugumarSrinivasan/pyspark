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

# 2.3. Find the top 5 most expensive products based on their price, along with their product name, category, and image URL.

# load the data using standard reader API
products_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/products/*")

# renaming the columns
column_renamed_df = products_df.withColumnRenamed("_c0", "ProductID").withColumnRenamed("_c1", "Category").withColumnRenamed("_c2", "ProductName").withColumnRenamed("_c3", "Description").withColumnRenamed("_c4", "Price").withColumnRenamed("_c5", "ImageURL")

# finding top5 products based on the Price through dataframe Way
# Method-1:
column_renamed_df.sort("Price", ascending = False).limit(5).show()

# Method-2:
results = column_renamed_df.select("ProductName", "Category", "Price", "ImageURL").orderBy("Price", ascending = False).limit(5)
results.show()

# finding top5 products based on the Price through SQL Way
column_renamed_df.createOrReplaceTempView("products")
results = spark.sql("select ProductName, Category, Price, ImageURL from products order by Price desc limit 5")
results.show()

spark.stop()
