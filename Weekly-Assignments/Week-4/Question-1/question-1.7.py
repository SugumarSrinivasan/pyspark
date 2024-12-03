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

orders_base = spark.sparkContext.textFile("/public/trendytech/retail_db/orders/*")

orders_map = orders_base.map(lambda x: (x.split(",")[2],1))
orders_reduce = orders_map.reduceByKey(lambda x,y: x+y)
orders_filter = orders_reduce.filter(lambda x: x[1] >=1)

orders_filter.count()

spark.stop()