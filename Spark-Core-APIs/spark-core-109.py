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

# without cache
orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")
orders_filtered = orders_base.filter(lambda x: x.split(",")[3]! = 'PENDING_PAYMENT')
orders_mapped = orders_filtered.map(lambda x: (x.split(",")[2],1))
orders_reduced = orders_mapped.reduceByKey(lambda x,y: x+y)
result = orders_reduced.filter(lambda x: int(x[0]) < 501)
result.collect()

# with cache
orders_base = spark.sparkContext.textFile("/public/trendytech/orders/orders_1gb.csv")
orders_filtered = orders_base.filter(lambda x: x.split(",")[3]! = 'PENDING_PAYMENT')
orders_mapped = orders_filtered.map(lambda x: (x.split(",")[2],1))
orders_reduced = orders_mapped.reduceByKey(lambda x,y: x+y)
result = orders_reduced.filter(lambda x: int(x[0]) < 501)
result.cache()
result.collect()