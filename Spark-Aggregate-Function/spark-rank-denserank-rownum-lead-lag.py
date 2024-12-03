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
.load("/public/trendytech/datasets/windowdatamodified.csv")

orders_df.show()

from pyspark.sql import *

orders_df.orderBy("country","invoicevalue").show(50)

# finding the running total

# Defining the window
mywindow = Window.partitionBy("country") \
.orderBy("weeknum") \
.rowsBetween(Window.unboundedPreceding, Window.currentRow)

from pyspark.sql.functions import *

results_df = orders_df.withColumn("running_total", sum("invoicevalue").over(mywindow))
results_df.show()

# finding the rank, dense rank and row num

# Defining the window
mywindow1 = Window.partitionBy("country") \
.orderBy(desc("invoicevalue")) \

# rank
result_df = orders_df.withColumn("rank", rank().over(mywindow1))
result_df.show()

# Denserank
result_df = orders_df.withColumn("denserank", dense_rank().over(mywindow1))
result_df.show()

# Row number
result_df = orders_df.withColumn("row_num", row_number().over(mywindow1))
result_df.show()

# find the top record from each group
result_df.select("*").where("rank == 1").show()

# find the top 3 records from each group
result_df.select("*").where("rank < 4").show()

# you to drop the column 'rank' because, we are applying the filter
result_df.select("*").where("rank == 1").drop("result").show()


# lead

window2 = Window.partitionBy("country") \
.orderBy("weeknum")

result_df = orders_df.withColumn("previous_week", lag("invoicevalue").over(window2))
result_df.show()

final_df = result_df.withColumn("invoice_diff", expr("invoicevalue - previous_week"))
final_df.show()

window3 = Window.partitionBy("country")
result = orders_df.withColumn("total_invoice_value", sum("invoicevalue").over(window3))
result.show()