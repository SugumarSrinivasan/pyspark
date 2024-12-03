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

# using reduceByKey
base_rdd = spark.sparkContext.textFile("/public/trendytech/orders/orders.csv")
mapped_rdd = base_rdd.map(lambda x: (x.split(",")[3],1))
reduced_rdd = mapped_rdd.reduceByKey(lambda x,y: x+y)
reduced_rdd.collect()

# using groupByKey
base_rdd = spark.sparkContext.textFile("/public/trendytech/orders/orders.csv")
mapped_rdd = base_rdd.map(lambda x: (x.split(",")[3],x.split(",")[2]))
groupped_rdd = mapped_rdd.groupByKey(lambda x,y: x+y)
results = groupped_rdd.map(lambda x: (x[0], len(x[1])))
results.collect()

spark.stop()