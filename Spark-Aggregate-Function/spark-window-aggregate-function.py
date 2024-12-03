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
.load("/public/trendytech/datasets/windowdata.csv")

orders_df.sort("country").show()

from pyspark.sql import *

# Defining the window

# 1. partition based on country
# 2. sort based on week number
# 3. The window size

mywindow = Window.partitionBy("country") \
.orderBy("weeknum") \
.rowsBeetween(Window.unboundedPreceding, Window.currentRow)

# -2 means it takes current row + 2 previous rows
mywindow = Window.partitionBy("country") \
.orderBy("weeknum") \
.rowsBeetween(-2, Window.currentRow)

result_df = orders_df.withColumn("running_total", sum("invoicevalue").over(mywindow))

result_df.show()