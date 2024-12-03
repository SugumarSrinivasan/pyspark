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

order_items_base = spark.sparkContext.textFile("/public/trendytech/retail_db/order_items/*")
order_items_map = order_items_base.map(lambda x: (int(x.split(",")[2]), int(x.split(",")[3])))
order_items_reduced = order_items_map.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending=False)
order_items_reduced.take(10)
spark.stop()