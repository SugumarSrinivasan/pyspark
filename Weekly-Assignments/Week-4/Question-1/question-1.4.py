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

customers_base = spark.sparkContext.textFile("/public/trendytech/retail_db/customers/*")
customers_map = customers_base.map(lambda x: (x.split(",")[7],1))
result = customers_map.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending = False)
result.take(3)
spark.stop()