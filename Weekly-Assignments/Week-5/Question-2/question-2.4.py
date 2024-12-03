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

# 2.4. Find the number of products in each category that have a price greater than $100. Display the results in a tabular format that shows the category name and the number of products that satisfy the condition.

# load the data using standard reader API
products_df = spark.read \
.format("csv") \
.option("header", "false") \
.option("inferSchema", "true") \
.load("/public/trendytech/retail_db/products/*")

# renaming the columns
column_renamed_df = products_df.withColumnRenamed("_c0", "ProductID").withColumnRenamed("_c1", "Category").withColumnRenamed("_c2", "ProductName").withColumnRenamed("_c3", "Description").withColumnRenamed("_c4", "Price").withColumnRenamed("_c5", "ImageURL")

results = column_renamed_df.filter("Price >100").groupBy("Category").count().withColumnRenamed("count", "Number_of_Products")
results.show()

column_renamed_df.createOrReplaceTempView("products")
results = spark.sql("select Category, count(*) as Number_of_Products from products where Price > 100 group by Category")
results.show()

spark.stop()